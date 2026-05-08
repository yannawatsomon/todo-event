package port

import (
	"context"

	"github.com/samber/mo"
	"go.mongodb.org/mongo-driver/v2/bson"

	"todoe/internal/authen/domain"
	"todoe/internal/event"
)

type UseCase interface {
	RegisterCredential(ctx context.Context, email, password string) mo.Result[domain.Credential]
	ActivateUser(ctx context.Context, userID, email, name string) mo.Result[domain.Credential]
	// UpdateEmail syncs a new email into the credential when the user updates
	// their contact info via the onboarding service (user.contact_updated event).
	UpdateEmail(ctx context.Context, userID, newEmail string) mo.Result[domain.Credential]
	Login(ctx context.Context, email, password string) mo.Result[domain.Session]
	Logout(ctx context.Context, token string) mo.Result[struct{}]
	ValidateToken(ctx context.Context, token string) mo.Result[domain.Session]
}

type Repository interface {
	Append(ctx context.Context, aggregateID bson.ObjectID, eventType string, payload any) mo.Result[struct{}]
	CreateCredential(ctx context.Context, cred domain.Credential) mo.Result[struct{}]
	FindCredentialByEmail(ctx context.Context, email string) mo.Result[domain.Credential]
	// FindCredentialByUserID looks up a credential by the onboarding user ID.
	// Used when syncing email changes from user.contact_updated events.
	FindCredentialByUserID(ctx context.Context, userID string) mo.Result[domain.Credential]
	// UpdateCredentialEmail replaces the email field on an existing credential.
	// Called when user.contact_updated arrives so login works with the new email.
	UpdateCredentialEmail(ctx context.Context, userID, newEmail string) mo.Result[struct{}]
	UpsertSession(ctx context.Context, session domain.Session) mo.Result[struct{}]
	FindActiveSessionByToken(ctx context.Context, token string) mo.Result[domain.Session]
	DeactivateSession(ctx context.Context, token string) mo.Result[struct{}]
}

type Publisher interface {
	Publish(ctx context.Context, e event.Event)
}
