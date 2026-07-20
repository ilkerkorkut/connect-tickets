package schema

import (
	"time"

	"github.com/google/uuid"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type OutboxEvent struct {
	ent.Schema
}

func (OutboxEvent) Mixin() []ent.Mixin {
	return []ent.Mixin{
		IDMixin{},
	}
}

func (OutboxEvent) Fields() []ent.Field {
	return []ent.Field{
		field.String("event_id").
			NotEmpty().
			MaxLen(128).
			Unique(),
		field.String("event_type").
			NotEmpty().
			MaxLen(64),
		field.UUID("aggregate_id", uuid.UUID{}),
		field.String("partition_key").
			NotEmpty().
			MaxLen(128),
		field.JSON("payload", map[string]any{}),
		field.Enum("status").
			Values("pending", "publishing", "published").
			Default("pending"),
		field.Time("available_at").
			Default(time.Now),
		field.String("publishing_owner").
			Optional().
			MaxLen(128),
		field.Time("publishing_lease_until").
			Optional().
			Nillable(),
		field.Int("publish_attempts").
			Default(0).
			NonNegative(),
		field.String("last_error").
			Optional().
			MaxLen(512),
		field.Time("published_at").
			Optional().
			Nillable(),
		field.Time("created_at").
			Immutable().
			Default(time.Now),
		field.Time("updated_at").
			Default(time.Now).
			UpdateDefault(time.Now),
	}
}

func (OutboxEvent) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("status", "available_at"),
		index.Fields("status", "publishing_lease_until"),
	}
}
