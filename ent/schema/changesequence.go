package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

type ChangeSequence struct {
	ent.Schema
}

func (ChangeSequence) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").
			NotEmpty().
			MaxLen(64).
			Unique(),
		field.Int64("last_value").
			Default(0).
			NonNegative(),
	}
}
