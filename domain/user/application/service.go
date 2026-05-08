package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/samber/mo"

	"todoe/domain/user/domain"
	"todoe/domain/user/port"
	"todoe/internal/event"
)

var (
	ErrInvalidName      = errors.New("name must not be empty")
	ErrInvalidEmail     = errors.New("email must not be empty")
	ErrInvalidToken     = errors.New("invalid verification token")
	ErrCreditNotChecked = errors.New("credit score not yet checked")
	ErrCreditDenied     = errors.New("credit application was denied")
	ErrEmailTaken       = errors.New("email is already in use by another user")
)

type Service struct {
	repo         port.Repository
	publisher    port.Publisher
	amqPublisher port.Publisher
}

var _ port.UseCase = (*Service)(nil)

func NewService(repo port.Repository, publisher port.Publisher, amqPublisher port.Publisher) *Service {
	return &Service{repo: repo, publisher: publisher, amqPublisher: amqPublisher}
}

func (s *Service) Register(ctx context.Context, name, email string) mo.Result[domain.User] {
	if name == "" {
		return mo.Err[domain.User](ErrInvalidName)
	}
	if email == "" {
		return mo.Err[domain.User](ErrInvalidEmail)
	}
	id := uuid.NewString()
	token := uuid.NewString()
	payload := domain.RegisteredPayload{Name: name, Email: email, VerificationToken: token}
	if r := s.repo.Append(ctx, id, domain.EventRegistered, payload); r.IsError() {
		return mo.Err[domain.User](r.Error())
	}
	user := domain.User{
		ID:                id,
		Name:              name,
		Email:             email,
		Status:            domain.StatusRegistered,
		VerificationToken: token,
		CreatedAt:         time.Now(),
	}
	s.publisher.Publish(ctx, event.Event{Type: domain.EventRegistered, Payload: user})
	s.amqPublisher.Publish(ctx, event.Event{Type: domain.EventRegistered, Payload: user})
	return mo.Ok(user)
}

func (s *Service) VerifyEmail(ctx context.Context, id, token string) mo.Result[domain.User] {
	current := s.repo.FindByID(ctx, id)
	if current.IsError() {
		return mo.Err[domain.User](current.Error())
	}
	u := current.MustGet()
	if u.VerificationToken != token {
		return mo.Err[domain.User](ErrInvalidToken)
	}
	next := u.WithEmailVerified()
	if r := s.repo.Append(ctx, id, domain.EventEmailVerified, domain.EmailVerifiedPayload{UserID: id}); r.IsError() {
		return mo.Err[domain.User](r.Error())
	}
	s.publisher.Publish(ctx, event.Event{Type: domain.EventEmailVerified, Payload: next})
	return mo.Ok(next)
}

func (s *Service) RecordCreditScore(ctx context.Context, id string, score int, approved bool) mo.Result[domain.User] {
	current := s.repo.FindByID(ctx, id)
	if current.IsError() {
		return mo.Err[domain.User](current.Error())
	}
	next := current.MustGet().WithCreditScore(score, approved)
	payload := domain.CreditScoredPayload{UserID: id, Score: score, Approved: approved}
	if r := s.repo.Append(ctx, id, domain.EventCreditScored, payload); r.IsError() {
		return mo.Err[domain.User](r.Error())
	}
	s.publisher.Publish(ctx, event.Event{Type: domain.EventCreditScored, Payload: next})
	return mo.Ok(next)
}

func (s *Service) GetUser(ctx context.Context, id string) mo.Result[domain.User] {
	return s.repo.FindByID(ctx, id)
}

func (s *Service) ListActivatedUsers(ctx context.Context) mo.Result[[]domain.User] {
	return s.repo.FindActivated(ctx)
}

func (s *Service) GetUserHistory(ctx context.Context, id string) mo.Result[[]domain.UserEvent] {
	return s.repo.FindEvents(ctx, id)
}

func (s *Service) UpdateContact(ctx context.Context, id, name, email, bio string) mo.Result[domain.User] {
	if name == "" {
		return mo.Err[domain.User](ErrInvalidName)
	}
	if email == "" {
		return mo.Err[domain.User](ErrInvalidEmail)
	}
	current := s.repo.FindByID(ctx, id)
	if current.IsError() {
		return mo.Err[domain.User](current.Error())
	}
	if found := s.repo.FindByEmail(ctx, email); !found.IsError() && found.MustGet().ID != id {
		return mo.Err[domain.User](ErrEmailTaken)
	}
	next := current.MustGet().WithContact(name, email, bio)
	payload := domain.ContactUpdatedPayload{UserID: id, Name: name, Email: email, Bio: bio}
	if r := s.repo.Append(ctx, id, domain.EventContactUpdated, payload); r.IsError() {
		return mo.Err[domain.User](r.Error())
	}
	// Publish ContactUpdatedPayload (not the User struct) so downstream consumers
	// like cmd/api can unmarshal p.UserID correctly via the json:"user_id" tag.
	//
	// Previously this published `next` (domain.User) which has json:"id" not
	// json:"user_id" — causing cmd/api to unmarshal UserID as "" and fail to
	// find the credential in auth_credentials.
	s.publisher.Publish(ctx, event.Event{Type: domain.EventContactUpdated, Payload: payload})
	return mo.Ok(next)
}

func (s *Service) CompleteProfile(ctx context.Context, id, bio string) mo.Result[domain.User] {
	current := s.repo.FindByID(ctx, id)
	if current.IsError() {
		return mo.Err[domain.User](current.Error())
	}
	u := current.MustGet()
	switch u.Status {
	case domain.StatusCreditDenied:
		return mo.Err[domain.User](ErrCreditDenied)
	case domain.StatusRegistered, domain.StatusEmailVerified:
		return mo.Err[domain.User](ErrCreditNotChecked)
	}
	next := u.WithProfile(bio)
	payload := domain.ProfileCompletedPayload{UserID: id, Bio: bio}
	if r := s.repo.Append(ctx, id, domain.EventProfileCompleted, payload); r.IsError() {
		return mo.Err[domain.User](r.Error())
	}
	s.publisher.Publish(ctx, event.Event{Type: domain.EventProfileCompleted, Payload: next})
	s.publisher.Publish(ctx, event.Event{
		Type: domain.EventUserActivated,
		Payload: domain.UserActivatedPayload{
			UserID: next.ID,
			Email:  next.Email,
			Name:   next.Name,
		},
	})
	s.amqPublisher.Publish(ctx, event.Event{
		Type: domain.EventUserActivated,
		Payload: domain.UserActivatedPayload{
			UserID: next.ID,
			Email:  next.Email,
			Name:   next.Name,
		},
	})
	return mo.Ok(next)
}
