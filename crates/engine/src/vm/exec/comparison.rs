// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	expression::{
		ColumnExpression, EqExpression, Expression, GreaterThanEqExpression, GreaterThanExpression,
		LessThanEqExpression, LessThanExpression, NotEqExpression,
	},
	interface::identifier::{ColumnIdentifier, ColumnObject},
	value::{
		batch::batch,
		column::{arith::length_mismatch, factory::rename},
	},
};
use reifydb_evaluate::{expression::logic::execute_logical_op, lower::LoweredExpr, stack::Variable};
use reifydb_value::{error::LogicalOp, fragment::Fragment};

use super::broadcast::broadcast_to_match;
use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	fn exec_columnar_cmp(&mut self, compare: fn(&CompareExprs) -> &LoweredExpr) -> Result<()> {
		let right = self.pop_as_column()?;
		let left = self.pop_as_column()?;
		let fragment = Fragment::internal("vm_cmp");
		let row_count = left.1.len();
		if row_count != right.1.len() {
			return Err(length_mismatch(row_count, right.1.len(), &fragment).into());
		}
		let operands = batch(vec![rename(left, "left"), rename(right, "right")])?;
		let ctx = self.eval_ctx();
		let result = compare(&self.compare).evaluate(&ctx.with_eval(operands, row_count))?;
		self.stack.push(Variable::columns(batch(vec![rename(result, fragment.text())])?));
		Ok(())
	}

	fn exec_columnar_logic(&mut self, op: LogicalOp) -> Result<()> {
		let right = self.pop_as_column()?;
		let left = self.pop_as_column()?;
		let (left, right) = broadcast_to_match(left, right)?;
		let frag = Fragment::internal("vm_logic");
		let result = execute_logical_op(&left, &right, &frag, op)?;
		self.stack.push(Variable::columns(batch(vec![result])?));
		Ok(())
	}

	pub(crate) fn exec_cmp_eq(&mut self) -> Result<()> {
		self.exec_columnar_cmp(|compare| &compare.eq)
	}

	pub(crate) fn exec_cmp_ne(&mut self) -> Result<()> {
		self.exec_columnar_cmp(|compare| &compare.ne)
	}

	pub(crate) fn exec_cmp_lt(&mut self) -> Result<()> {
		self.exec_columnar_cmp(|compare| &compare.lt)
	}

	pub(crate) fn exec_cmp_le(&mut self) -> Result<()> {
		self.exec_columnar_cmp(|compare| &compare.le)
	}

	pub(crate) fn exec_cmp_gt(&mut self) -> Result<()> {
		self.exec_columnar_cmp(|compare| &compare.gt)
	}

	pub(crate) fn exec_cmp_ge(&mut self) -> Result<()> {
		self.exec_columnar_cmp(|compare| &compare.ge)
	}

	pub(crate) fn exec_logic_and(&mut self) -> Result<()> {
		self.exec_columnar_logic(LogicalOp::And)
	}

	pub(crate) fn exec_logic_or(&mut self) -> Result<()> {
		self.exec_columnar_logic(LogicalOp::Or)
	}

	pub(crate) fn exec_logic_xor(&mut self) -> Result<()> {
		self.exec_columnar_logic(LogicalOp::Xor)
	}
}

pub(crate) struct CompareExprs {
	eq: LoweredExpr,
	ne: LoweredExpr,
	lt: LoweredExpr,
	le: LoweredExpr,
	gt: LoweredExpr,
	ge: LoweredExpr,
}

impl CompareExprs {
	pub(crate) fn new() -> Self {
		let (left, right, fragment) = (operand("left"), operand("right"), Fragment::internal("vm_cmp"));
		let lowered = |expression| LoweredExpr::new(expression, "vm_cmp");
		Self {
			eq: lowered(Expression::Equal(EqExpression {
				left: left.clone(),
				right: right.clone(),
				fragment: fragment.clone(),
			})),
			ne: lowered(Expression::NotEqual(NotEqExpression {
				left: left.clone(),
				right: right.clone(),
				fragment: fragment.clone(),
			})),
			lt: lowered(Expression::LessThan(LessThanExpression {
				left: left.clone(),
				right: right.clone(),
				fragment: fragment.clone(),
			})),
			le: lowered(Expression::LessThanEqual(LessThanEqExpression {
				left: left.clone(),
				right: right.clone(),
				fragment: fragment.clone(),
			})),
			gt: lowered(Expression::GreaterThan(GreaterThanExpression {
				left: left.clone(),
				right: right.clone(),
				fragment: fragment.clone(),
			})),
			ge: lowered(Expression::GreaterThanEqual(GreaterThanEqExpression {
				left,
				right,
				fragment,
			})),
		}
	}
}

fn operand(name: &str) -> Box<Expression> {
	Box::new(Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Alias(Fragment::internal("vm")),
		name: Fragment::internal(name),
	})))
}
