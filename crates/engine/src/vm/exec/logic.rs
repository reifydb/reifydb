// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	expression::{BetweenExpression, Expression, InExpression, TupleExpression},
	value::{
		batch::batch,
		column::{arith::length_mismatch, cast::cast_column_data, factory::rename},
	},
};
use reifydb_evaluate::{lower::LoweredExpr, stack::Variable};
use reifydb_value::{
	fragment::Fragment,
	value::{column_view::ColumnView, value_type::ValueType},
};

use super::comparison::operand;
use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn exec_between(&mut self) -> Result<()> {
		let upper = self.pop_as_column()?;
		let lower = self.pop_as_column()?;
		let value = self.pop_as_column()?;
		let fragment = Fragment::internal("vm_between");
		let row_count = value.1.len();
		for bound in [&lower, &upper] {
			if bound.1.len() != row_count {
				return Err(length_mismatch(row_count, bound.1.len(), &fragment).into());
			}
		}
		let operands = batch(vec![rename(value, "value"), rename(lower, "lower"), rename(upper, "upper")])?;
		let ctx = self.eval_ctx();
		let result = self.membership.between.evaluate(&ctx.with_eval(operands, row_count))?;
		self.stack.push(Variable::columns(batch(vec![rename(result, fragment.text())])?));
		Ok(())
	}

	pub(crate) fn exec_in_list(&mut self, count: u16, negated: bool) -> Result<()> {
		let mut items = Vec::with_capacity(count as usize);
		for _ in 0..count {
			items.push(self.pop_as_column()?);
		}
		items.reverse();
		let value = self.pop_as_column()?;
		let fragment = Fragment::internal("vm_in_list");
		let row_count = value.1.len();
		let mut operands = vec![rename(value, "value")];
		for (index, item) in items.into_iter().enumerate() {
			if item.1.len() != row_count {
				return Err(length_mismatch(row_count, item.1.len(), &fragment).into());
			}
			operands.push(rename(item, &format!("item{index}")));
		}
		let operands = batch(operands)?;
		self.membership.in_list.entry((count, negated)).or_insert_with(|| in_list(count, negated));
		let ctx = self.eval_ctx();
		let result =
			self.membership.in_list[&(count, negated)].evaluate(&ctx.with_eval(operands, row_count))?;
		self.stack.push(Variable::columns(batch(vec![rename(result, fragment.text())])?));
		Ok(())
	}

	pub(crate) fn exec_cast(&mut self, target: &ValueType) -> Result<()> {
		let col = self.pop_as_column()?;
		let frag = Fragment::internal("vm_cast");
		let ctx = self.eval_ctx();
		let cast = cast_column_data(&ctx, &ColumnView::try_from(&col)?, target.clone(), frag.clone())?;
		self.stack.push(Variable::columns(batch(vec![cast])?));
		Ok(())
	}
}

pub(crate) struct MembershipExprs {
	between: LoweredExpr,
	in_list: HashMap<(u16, bool), LoweredExpr>,
}

impl MembershipExprs {
	pub(crate) fn new() -> Self {
		Self {
			between: LoweredExpr::new(
				Expression::Between(BetweenExpression {
					value: operand("value"),
					lower: operand("lower"),
					upper: operand("upper"),
					fragment: Fragment::internal("vm_between"),
				}),
				"vm_between",
			),
			in_list: HashMap::new(),
		}
	}
}

fn in_list(count: u16, negated: bool) -> LoweredExpr {
	let fragment = Fragment::internal("vm_in_list");
	LoweredExpr::new(
		Expression::In(InExpression {
			value: operand("value"),
			list: Box::new(Expression::Tuple(TupleExpression {
				expressions: (0..count).map(|index| *operand(&format!("item{index}"))).collect(),
				fragment: fragment.clone(),
			})),
			negated,
			fragment,
		}),
		"vm_in_list",
	)
}
