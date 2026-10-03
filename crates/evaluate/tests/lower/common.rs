// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, Schema};
use reifydb_core::{
	expression::{
		AccessObjectExpression, AddExpression, AndExpression, BetweenExpression, ColumnExpression,
		ConstantExpression, DivExpression, EqExpression, Expression, GreaterThanEqExpression,
		GreaterThanExpression, LessThanEqExpression, LessThanExpression, MulExpression, NotEqExpression,
		OrExpression, PrefixExpression, PrefixOperator, RemExpression, SubExpression, XorExpression,
	},
	interface::identifier::{ColumnIdentifier, ColumnObject},
	value::column::factory,
};
use reifydb_evaluate::{
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
	lower::LoweredExpr,
	stack::SymbolTable,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	Result,
	fragment::Fragment,
	params::Params,
	value::{column_view::ColumnView, identity::IdentityId},
};

pub struct Env {
	params: Params,
	pub symbols: SymbolTable,
	routines: Routines,
	runtime_context: RuntimeContext,
}

impl Env {
	pub fn new() -> Self {
		Self::with_symbols(SymbolTable::new())
	}

	pub fn with_symbols(symbols: SymbolTable) -> Self {
		Self {
			params: Params::None,
			symbols,
			routines: Routines::empty(),
			runtime_context: RuntimeContext::with_clock(Clock::Real),
		}
	}

	pub fn ctx(&self, batch: RecordBatch) -> EvalContext<'_> {
		let row_count = batch.num_rows();
		EvalContext {
			target: None,
			batch,
			row_count,
			take: None,
			params: &self.params,
			symbols: &self.symbols,
			is_aggregate_context: false,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: IdentityId::root(),
		}
	}

	pub fn lowered(&self, expression: &Expression, batch: RecordBatch) -> Result<(FieldRef, ArrayRef)> {
		LoweredExpr::new(expression.clone(), "test").evaluate(&self.ctx(batch))
	}

	pub fn old(&self, expression: &Expression, batch: RecordBatch) -> Result<(FieldRef, ArrayRef)> {
		let compile_ctx = CompileContext {
			symbols: &self.symbols,
		};
		compile_expression(&compile_ctx, expression)?.execute(&self.ctx(batch))
	}
}

pub fn batch(columns: Vec<(FieldRef, ArrayRef)>) -> RecordBatch {
	let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

pub fn rows(count: usize) -> RecordBatch {
	batch(vec![factory::int4("filler", vec![0; count])])
}

pub fn strings(result: &(FieldRef, ArrayRef)) -> Vec<String> {
	let view = ColumnView::try_from(result).unwrap();
	(0..result.1.len()).map(|i| view.as_string(i)).collect()
}

pub fn frag(text: &str) -> Fragment {
	Fragment::internal(text)
}

pub fn column(name: &str) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Alias(frag("t")),
		name: frag(name),
	}))
}

pub fn access(object: ColumnObject, name: &str) -> Expression {
	Expression::AccessSource(AccessObjectExpression {
		column: ColumnIdentifier {
			object,
			name: frag(name),
		},
	})
}

pub fn none() -> Expression {
	Expression::Constant(ConstantExpression::None {
		fragment: frag("none"),
	})
}

pub fn boolean(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Bool {
		fragment: frag(text),
	})
}

pub fn number(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Number {
		fragment: frag(text),
	})
}

pub fn text(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Text {
		fragment: frag(text),
	})
}

pub fn temporal(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Temporal {
		fragment: frag(text),
	})
}

pub fn duration(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Duration {
		fragment: frag(text),
	})
}

pub fn and(left: Expression, right: Expression) -> Expression {
	Expression::And(AndExpression {
		left: Box::new(left),
		right: Box::new(right),
		fragment: frag("and"),
	})
}

pub fn or(left: Expression, right: Expression) -> Expression {
	Expression::Or(OrExpression {
		left: Box::new(left),
		right: Box::new(right),
		fragment: frag("or"),
	})
}

pub fn xor(left: Expression, right: Expression) -> Expression {
	Expression::Xor(XorExpression {
		left: Box::new(left),
		right: Box::new(right),
		fragment: frag("xor"),
	})
}

pub fn not(inner: Expression) -> Expression {
	prefix(PrefixOperator::Not(frag("not")), inner)
}

pub fn prefix(operator: PrefixOperator, inner: Expression) -> Expression {
	Expression::Prefix(PrefixExpression {
		operator,
		expression: Box::new(inner),
		fragment: frag("prefix"),
	})
}

pub const COMPARE_OPS: [&str; 6] = ["==", "!=", "<", "<=", ">", ">="];

pub fn compare(op: &str, left: Expression, right: Expression) -> Expression {
	let (left, right, fragment) = (Box::new(left), Box::new(right), frag(op));
	match op {
		"==" => Expression::Equal(EqExpression {
			left,
			right,
			fragment,
		}),
		"!=" => Expression::NotEqual(NotEqExpression {
			left,
			right,
			fragment,
		}),
		"<" => Expression::LessThan(LessThanExpression {
			left,
			right,
			fragment,
		}),
		"<=" => Expression::LessThanEqual(LessThanEqExpression {
			left,
			right,
			fragment,
		}),
		">" => Expression::GreaterThan(GreaterThanExpression {
			left,
			right,
			fragment,
		}),
		">=" => Expression::GreaterThanEqual(GreaterThanEqExpression {
			left,
			right,
			fragment,
		}),
		other => panic!("no compare operator {other}"),
	}
}

pub fn between(value: Expression, lower: Expression, upper: Expression) -> Expression {
	Expression::Between(BetweenExpression {
		value: Box::new(value),
		lower: Box::new(lower),
		upper: Box::new(upper),
		fragment: frag("between"),
	})
}

pub const ARITH_OPS: [&str; 5] = ["+", "-", "*", "/", "%"];

pub fn arith(op: &str, left: Expression, right: Expression) -> Expression {
	let (left, right, fragment) = (Box::new(left), Box::new(right), frag(op));
	match op {
		"+" => Expression::Add(AddExpression {
			left,
			right,
			fragment,
		}),
		"-" => Expression::Sub(SubExpression {
			left,
			right,
			fragment,
		}),
		"*" => Expression::Mul(MulExpression {
			left,
			right,
			fragment,
		}),
		"/" => Expression::Div(DivExpression {
			left,
			right,
			fragment,
		}),
		"%" => Expression::Rem(RemExpression {
			left,
			right,
			fragment,
		}),
		other => panic!("no arithmetic operator {other}"),
	}
}
