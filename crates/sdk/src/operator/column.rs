//! Column definitions for operator input/output schemas

use reifydb_type::TypeConstraint;

/// A single column definition in an operator's input/output
#[derive(Debug, Clone)]
pub struct OperatorColumnDef {
	/// Column name
	pub name: &'static str,
	/// Column type constraint (use TypeConstraint::unconstrained(Type::X) for unconstrained types)
	pub field_type: TypeConstraint,
	/// Human-readable description
	pub description: &'static str,
}
