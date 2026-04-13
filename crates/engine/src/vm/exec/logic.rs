// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_rql::expression::PrefixOperator;
use reifydb_type::{
	error::{BinaryOp, IntoDiagnostic, LogicalOp, TypeError},
	fragment::Fragment,
	value::r#type::Type,
};

use super::broadcast::broadcast_many;
use crate::{
	Result,
	expression::{
		cast::cast_column_data,
		compare::{Equal, GreaterThanEqual, LessThanEqual, compare_columns},
		logic::execute_logical_op,
		prefix::prefix_apply,
	},
	vm::{stack::Variable, vm::Vm},
};

impl<'a> Vm<'a> {
	pub(crate) fn exec_between(&mut self) -> Result<()> {
		let upper = self.pop_as_column()?;
		let lower = self.pop_as_column()?;
		let value = self.pop_as_column()?;
		let cols = broadcast_many(vec![value, lower, upper]);
		let mut iter = cols.into_iter();
		let value = iter.next().unwrap();
		let lower = iter.next().unwrap();
		let upper = iter.next().unwrap();

		let frag = Fragment::internal("vm_between");
		let ge = compare_columns::<GreaterThanEqual>(&value, &lower, frag.clone(), |frag, lt, rt| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::GreaterThanEqual,
				left: lt,
				right: rt,
				fragment: frag,
			}
			.into_diagnostic()
		})?;
		let le = compare_columns::<LessThanEqual>(&value, &upper, frag.clone(), |frag, lt, rt| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::LessThanEqual,
				left: lt,
				right: rt,
				fragment: frag,
			}
			.into_diagnostic()
		})?;
		let result = execute_logical_op(&ge, &le, &frag, LogicalOp::And, |a, b| a && b)?;
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}

	pub(crate) fn exec_in_list(&mut self, count: u16, negated: bool) -> Result<()> {
		let count = count as usize;
		let mut list_items = Vec::with_capacity(count);
		for _ in 0..count {
			list_items.push(self.pop_as_column()?);
		}
		list_items.reverse();
		let probe = self.pop_as_column()?;

		let mut all = Vec::with_capacity(count + 1);
		all.push(probe);
		all.extend(list_items);
		let mut all = broadcast_many(all);
		let probe = all.remove(0);
		let list_items = all;

		let frag = Fragment::internal("vm_in_list");
		let mut accumulator: Option<Column> = None;
		for item in &list_items {
			let eq = compare_columns::<Equal>(&probe, item, frag.clone(), |frag, lt, rt| {
				TypeError::BinaryOperatorNotApplicable {
					operator: BinaryOp::Equal,
					left: lt,
					right: rt,
					fragment: frag,
				}
				.into_diagnostic()
			})?;
			accumulator = Some(match accumulator {
				None => eq,
				Some(acc) => execute_logical_op(&acc, &eq, &frag, LogicalOp::Or, |a, b| a || b)?,
			});
		}

		let result = match accumulator {
			Some(col) => {
				if negated {
					prefix_apply(&col, &PrefixOperator::Not(frag.clone()), &frag)?
				} else {
					col
				}
			}
			None => {
				// Empty list: IN is always false, NOT IN always true (broadcast to probe length).
				let len = probe.data.len().max(1);
				let data = ColumnData::bool(vec![negated; len]);
				Column::new(frag.clone(), data)
			}
		};
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}

	pub(crate) fn exec_cast(&mut self, target: &Type) -> Result<()> {
		let col = self.pop_as_column()?;
		let frag = Fragment::internal("vm_cast");
		let ctx = self.eval_ctx();
		let data = cast_column_data(&ctx, col.data(), target.clone(), frag.clone())?;
		self.stack.push(Variable::columns(Columns::new(vec![Column::new(col.name().clone(), data)])));
		Ok(())
	}
}
