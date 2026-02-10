// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_rql::expression::{
	AccessPrimitiveExpression, CallExpression, ColumnExpression, ConstantExpression, ParameterExpression,
	PrefixExpression, VariableExpression,
};
use reifydb_type::{fragment::Fragment, value::r#type::Type};

pub enum CompiledExpr {
	Constant(ConstantExpression),

	// Leaf nodes
	Column(ColumnExpression),
	Variable(VariableExpression),
	Parameter(ParameterExpression),

	// Unary
	Alias {
		inner: Box<CompiledExpr>,
		alias: Fragment,
	},

	// Binary arithmetic
	Add {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	Sub {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	Mul {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	Div {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	Rem {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},

	// Comparisons
	Equal {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	NotEqual {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	GreaterThan {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	GreaterThanEqual {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	LessThan {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	LessThanEqual {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},

	// Logic
	And {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	Or {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},
	Xor {
		left: Box<CompiledExpr>,
		right: Box<CompiledExpr>,
		fragment: Fragment,
	},

	// Prefix
	Prefix(PrefixExpression),

	// Type
	Type {
		ty: Type,
		fragment: Fragment,
	},

	// Access + Tuple
	AccessSource(AccessPrimitiveExpression),
	Tuple {
		inner: Box<CompiledExpr>,
	},

	// Between + In
	Between {
		value: Box<CompiledExpr>,
		lower: Box<CompiledExpr>,
		upper: Box<CompiledExpr>,
		fragment: Fragment,
	},
	In {
		value: Box<CompiledExpr>,
		list: Vec<CompiledExpr>,
		negated: bool,
		fragment: Fragment,
	},

	// Cast
	Cast {
		inner: Box<CompiledExpr>,
		target_type: Type,
		inner_fragment: Fragment,
	},

	// If
	If {
		condition: Box<CompiledExpr>,
		then_expr: Vec<CompiledExpr>,
		else_ifs: Vec<(Box<CompiledExpr>, Vec<CompiledExpr>)>,
		else_branch: Option<Vec<CompiledExpr>>,
		fragment: Fragment,
	},

	// Map + Extend
	Map {
		expressions: Vec<CompiledExpr>,
	},
	Extend {
		expressions: Vec<CompiledExpr>,
	},

	// Call (delegates to evaluator)
	Call(CallExpression),
}
