package adapter

import (
	"context"
	"fmt"

	"todoe/domain/user/domain"
	"todoe/internal/event"
)

func NewProjectionHandler(repo *MySQLRepository) func(context.Context, event.Event) error {
	return func(ctx context.Context, e event.Event) error {
		// Most events publish domain.User as payload so the projection can
		// upsert the full read model directly.
		//
		// user.contact_updated is the exception — it publishes ContactUpdatedPayload
		// (a struct with only the changed fields) because the service needs to
		// broadcast a typed payload for downstream consumers (e.g. cmd/api email sync).
		// We reconstruct a partial User from it so Upsert() can update the read model.
		var user domain.User

		switch p := e.Payload.(type) {
		case domain.User:
			user = p
		case domain.ContactUpdatedPayload:
			// Only name/email/bio change — other fields (status, credit, etc.) are
			// preserved by Upsert()'s UPDATE ... SET name=?, email=?, bio=? WHERE id=?
			user = domain.User{
				ID:    p.UserID,
				Name:  p.Name,
				Email: p.Email,
				Bio:   p.Bio,
			}
		default:
			return fmt.Errorf("unexpected payload type %T", e.Payload)
		}

		result := repo.Upsert(ctx, user)
		if result.IsError() {
			return result.Error()
		}
		return nil
	}
}
