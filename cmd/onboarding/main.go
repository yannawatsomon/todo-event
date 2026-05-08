package main

import (
	"context"
	"hash/fnv"
	"log"
	"log/slog"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"github.com/jmoiron/sqlx"
	"github.com/samber/mo"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	captchaadapter "todoe/internal/captcha/adapter"
	captchahttp "todoe/internal/captcha/adapter/http"
	captchaapp "todoe/internal/captcha/application"
	captchadomain "todoe/internal/captcha/domain"

	useradapter "todoe/domain/user/adapter"
	userhttp "todoe/domain/user/adapter/http"
	userapplication "todoe/domain/user/application"
	userdomain "todoe/domain/user/domain"

	"todoe/internal/event"
	"todoe/internal/messaging"
)

type multiPublisher struct{ publishers []event.Publisher }

func (m *multiPublisher) Publish(ctx context.Context, e event.Event) {
	for _, p := range m.publishers {
		p.Publish(ctx, e)
	}
}

func fakeCreditScore(email string) (int, bool) {
	h := fnv.New32a()
	h.Write([]byte(email))
	score := 500 + int(h.Sum32()%551)
	return score, score >= 600
}

func main() {
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		// Port 3307 — local MySQL84 occupies 3306, Docker MySQL is on 3307
		mysqlDSN = "todoe:todoe@tcp(localhost:3307)/todoe_onboarding?parseTime=true&multiStatements=true"
	}
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://root:root@localhost:27017"
	}
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	db, err := sqlx.ConnectContext(context.Background(), "mysql", mysqlDSN)
	if err != nil {
		log.Fatal("mysql:", err)
	}
	defer db.Close()

	mongoClientIO := mo.NewIOEither(func() (*mongo.Client, error) {
		return mongo.Connect(options.Client().ApplyURI(mongoURI))
	})

	conn, ch, err := messaging.Connect(amqpURL)
	if err != nil {
		log.Fatal("rabbit:", err)
	}
	defer conn.Close()

	if err := messaging.DeclareTopology(ch, []messaging.Binding{
		{Exchange: messaging.OnboardingExchange, Queue: messaging.QueueAuditUserEvents},
		{Exchange: messaging.UserExchange, Queue: messaging.QueueAuthenUserEvents},
	}); err != nil {
		log.Fatal("rabbit topology:", err)
	}

	if err := useradapter.Migrate(db); err != nil {
		log.Fatal("mysql migrate:", err)
	}
	userRepo := useradapter.NewMySQLRepository(db)

	userBus := event.NewEventBus()
	userProjection := useradapter.NewProjectionHandler(userRepo)
	userBus.Subscribe(userdomain.EventRegistered, userProjection)
	userBus.Subscribe(userdomain.EventEmailVerified, userProjection)
	userBus.Subscribe(userdomain.EventCreditScored, userProjection)
	userBus.Subscribe(userdomain.EventProfileCompleted, userProjection)
	userBus.Subscribe(userdomain.EventContactUpdated, userProjection)

	userPublisher := &multiPublisher{publishers: []event.Publisher{
		userBus,
		messaging.NewPublisher(ch, messaging.UserExchange),
	}}

	userService := userapplication.NewService(userRepo, userPublisher, messaging.NewPublisher(ch, messaging.OnboardingExchange))
	userHandler := userhttp.NewHandler(userService)

	// Credit scoring runs in-process: on email_verified → score → RecordCreditScore
	userBus.Subscribe(userdomain.EventEmailVerified, func(ctx context.Context, e event.Event) error {
		user, ok := e.Payload.(userdomain.User)
		if !ok {
			return nil
		}
		score, approved := fakeCreditScore(user.Email)
		slog.Info("credit: scored", "user_id", user.ID, "score", score, "approved", approved)
		if r := userService.RecordCreditScore(ctx, user.ID, score, approved); r.IsError() {
			slog.Error("credit: record score failed", "err", r.Error())
		}
		return nil
	})

	// Welcome logging: onboarding milestones via local event bus
	userBus.Subscribe(userdomain.EventRegistered, func(_ context.Context, e event.Event) error {
		user, _ := e.Payload.(userdomain.User)
		slog.Info("step 1/4: verification email sent", "to", user.Email, "name", user.Name, "token", user.VerificationToken)
		return nil
	})
	userBus.Subscribe(userdomain.EventEmailVerified, func(_ context.Context, e event.Event) error {
		user, _ := e.Payload.(userdomain.User)
		slog.Info("step 2/4: email confirmed — running credit check...", "user_id", user.ID)
		return nil
	})
	userBus.Subscribe(userdomain.EventCreditScored, func(_ context.Context, e event.Event) error {
		user, _ := e.Payload.(userdomain.User)
		if user.CreditApproved {
			slog.Info("step 3/4: credit approved — complete your profile", "score", user.CreditScore)
		} else {
			slog.Info("step 3/4: credit denied — onboarding blocked", "score", user.CreditScore)
		}
		return nil
	})
	userBus.Subscribe(userdomain.EventProfileCompleted, func(_ context.Context, e event.Event) error {
		user, _ := e.Payload.(userdomain.User)
		slog.Info("step 4/4: onboarding complete — welcome!", "name", user.Name, "bio", user.Bio)
		return nil
	})
	userBus.Subscribe(userdomain.EventContactUpdated, func(_ context.Context, e event.Event) error {
		// Payload is ContactUpdatedPayload (not domain.User) — see UpdateContact()
		p, _ := e.Payload.(userdomain.ContactUpdatedPayload)
		slog.Info("contact updated", "user_id", p.UserID, "name", p.Name, "email", p.Email)
		return nil
	})

	// ── Captcha domain ───────────────────────────────────────────────────
	captchaBus := event.NewEventBus()
	captchaRepo := captchaadapter.NewMongoRepository(mongoClientIO)
	captchaProjection := captchaadapter.NewProjectionHandler(captchaRepo)
	captchaBus.Subscribe(captchadomain.EventIssued, captchaProjection)
	captchaBus.Subscribe(captchadomain.EventVerified, captchaProjection)
	captchaService := captchaapp.NewService(captchaRepo, captchaBus)
	captchaHandler := captchahttp.NewHandler(captchaService)

	// ── HTTP ─────────────────────────────────────────────────────────────
	app := fiber.New()

	app.Post("/users/register", userHandler.Register)
	app.Get("/users/activated", userHandler.ListActivated)
	app.Get("/users/:id", userHandler.GetUser)
	app.Get("/users/:id/history", userHandler.GetHistory)
	app.Patch("/users/:id", userHandler.UpdateContact)
	app.Post("/users/:id/verify-email", userHandler.VerifyEmail)
	app.Post("/users/:id/complete-profile", userHandler.CompleteProfile)

	app.Post("/captcha", captchaHandler.Issue)
	app.Post("/captcha/:id/verify", captchaHandler.Verify)

	slog.Info("onboarding service listening", "port", "3003")
	log.Fatal(app.Listen(":3003"))
}
