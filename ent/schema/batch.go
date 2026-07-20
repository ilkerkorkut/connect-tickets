package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

type Batch struct {
	ent.Schema
}

func (Batch) Mixin() []ent.Mixin {
	return []ent.Mixin{
		IDMixin{},
	}
}

func (Batch) Fields() []ent.Field {
	return []ent.Field{
		field.Time("created_at").
			Immutable(),
		field.Time("completed_at").
			Optional().
			Nillable(),
	}
}
