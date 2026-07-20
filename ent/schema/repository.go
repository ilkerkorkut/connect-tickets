package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type Repository struct {
	ent.Schema
}

func (Repository) Mixin() []ent.Mixin {
	return []ent.Mixin{
		IDMixin{},
		TimeMixin{},
	}
}

func (Repository) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Check("stars >= 0"),
	}
}

func (Repository) Fields() []ent.Field {
	return []ent.Field{
		field.String("owner").
			NotEmpty().
			MaxLen(255),
		field.String("name").
			NotEmpty().
			MaxLen(255),
		field.String("full_name").
			NotEmpty().
			MaxLen(512),
		field.String("normalized_full_name").
			NotEmpty().
			MaxLen(512),
		field.String("description").
			Optional().
			MaxLen(4096),
		field.Int("stars").
			Default(0).
			NonNegative(),
		field.String("language").
			Optional().
			MaxLen(128),
		field.String("html_url").
			NotEmpty().
			MaxLen(2048),
		field.String("notes").
			Default("").
			MaxLen(4000),
		field.Time("fetched_at").
			Default(time.Now),
		field.Time("deleted_at").
			Optional().
			Nillable(),
	}
}

func (Repository) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("normalized_full_name").
			Unique().
			Annotations(entsql.IndexWhere("deleted_at IS NULL")),
		index.Fields("language"),
		index.Fields("stars"),
		index.Fields("updated_at"),
		index.Fields("fetched_at"),
	}
}
