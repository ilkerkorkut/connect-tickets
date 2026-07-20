package schema

import (
	"time"

	"github.com/google/uuid"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type BatchJob struct {
	ent.Schema
}

func (BatchJob) Mixin() []ent.Mixin {
	return []ent.Mixin{
		IDMixin{},
	}
}

func (BatchJob) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("batch_id", uuid.UUID{}),
		field.UUID("repo_id", uuid.UUID{}),
		field.Enum("status").
			Values(
				"pending",
				"queued",
				"processing",
				"retry_wait",
				"succeeded",
				"failed",
			).
			Default("pending"),
		field.Int("attempt_count").
			Default(0).
			NonNegative(),
		field.String("processing_owner").
			Optional().
			MaxLen(128),
		field.Time("lease_until").
			Optional().
			Nillable(),
		field.Time("next_attempt_at").
			Optional().
			Nillable(),
		field.String("last_error_code").
			Optional().
			MaxLen(64),
		field.String("last_error_message").
			Optional().
			MaxLen(512),
		field.Time("created_at").
			Immutable().
			Default(time.Now),
		field.Time("started_at").
			Optional().
			Nillable(),
		field.Time("completed_at").
			Optional().
			Nillable(),
		field.Time("updated_at").
			Default(time.Now).
			UpdateDefault(time.Now),
	}
}

func (BatchJob) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("batch_id", "repo_id").Unique(),
		index.Fields("batch_id", "status"),
		index.Fields("status", "next_attempt_at"),
		index.Fields("status", "lease_until"),
		index.Fields("repo_id", "status"),
	}
}
