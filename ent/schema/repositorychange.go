package schema

import (
	"time"

	"github.com/google/uuid"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type RepositoryChange struct {
	ent.Schema
}

func (RepositoryChange) Mixin() []ent.Mixin {
	return []ent.Mixin{
		IDMixin{},
	}
}

func (RepositoryChange) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("sequence").
			Unique(),
		field.String("event_id").
			NotEmpty().
			MaxLen(128).
			Unique(),
		field.UUID("repo_id", uuid.UUID{}).
			Optional().
			Nillable(),
		field.Enum("operation").
			Values("created", "notes_updated", "refreshed", "deleted"),
		field.JSON("repository_snapshot", map[string]any{}).
			Optional(),
		field.Time("changed_at").
			Default(time.Now),
	}
}

func (RepositoryChange) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("repo_id", "changed_at"),
		index.Fields("operation", "changed_at"),
	}
}
