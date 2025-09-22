// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::OrExpression},
	return_error,
	value::columnar::{Column, ColumnComputed, ColumnData},
};
use reifydb_type::diagnostic::operator::{
	or_can_not_applied_to_number, or_can_not_applied_to_temporal, or_can_not_applied_to_text,
	or_can_not_applied_to_uuid,
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn or<'a>(&self, ctx: &EvaluationContext<'a>, expr: &OrExpression<'a>) -> crate::Result<Column<'a>> {
		let left = self.evaluate(ctx, &expr.left)?;
		let right = self.evaluate(ctx, &expr.right)?;

		match (&left.data(), &right.data()) {
			(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
				if l_container.is_fully_defined() && r_container.is_fully_defined() {
					// Fast path: all values are defined, no
					// undefined checks needed
					let data: Vec<bool> = l_container
						.data()
						.iter()
						.zip(r_container.data().iter())
						.map(|(l_val, r_val)| l_val || r_val)
						.collect();

					Ok(Column::Computed(ColumnComputed {
						name: expr.full_fragment_owned(),
						data: ColumnData::bool(data),
					}))
				} else {
					// Slow path: some values may be
					// undefined
					let mut data = Vec::with_capacity(l_container.data().len());
					let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

					for i in 0..l_container.data().len() {
						if l_container.is_defined(i) && r_container.is_defined(i) {
							data.push(
								l_container.data().get(i) || r_container.data().get(i)
							);
							bitvec.push(true);
						} else {
							data.push(false);
							bitvec.push(false);
						}
					}

					Ok(Column::Computed(ColumnComputed {
						name: expr.full_fragment_owned(),
						data: ColumnData::bool_with_bitvec(data, bitvec),
					}))
				}
			}
			(l, r) => {
				if l.is_number() || r.is_number() {
					return_error!(or_can_not_applied_to_number(expr.full_fragment_owned()));
				} else if l.is_text() || r.is_text() {
					return_error!(or_can_not_applied_to_text(expr.full_fragment_owned()));
				} else if l.is_temporal() || r.is_temporal() {
					return_error!(or_can_not_applied_to_temporal(expr.full_fragment_owned()));
				} else {
					return_error!(or_can_not_applied_to_uuid(expr.full_fragment_owned()));
				}
			}
		}
	}
}
