package schema

import (
	"github.com/google/uuid"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type JobAttempt struct {
	ent.Schema
}

func (JobAttempt) Mixin() []ent.Mixin {
	return []ent.Mixin{
		IDMixin{},
	}
}

func (JobAttempt) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("batch_job_id", uuid.UUID{}),
		field.Int("attempt_number").
			Positive(),
		field.String("worker_id").
			NotEmpty().
			MaxLen(128),
		field.Time("started_at"),
		field.Time("finished_at").
			Optional().
			Nillable(),
		field.Enum("result").
			Values("succeeded", "failed", "retry_scheduled"),
		field.String("error_code").
			Optional().
			MaxLen(64),
		field.Int("duration_ms").
			Default(0).
			NonNegative(),
	}
}

func (JobAttempt) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("batch_job_id", "attempt_number").Unique(),
	}
}
