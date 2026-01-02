// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	return_error,
	value::column::{Column, ColumnData},
};
use reifydb_rql::expression::AndExpression;
use reifydb_type::diagnostic::operator::{
	and_can_not_applied_to_number, and_can_not_applied_to_temporal, and_can_not_applied_to_text,
	and_can_not_applied_to_uuid,
};

use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn and(&self, ctx: &ColumnEvaluationContext, expr: &AndExpression) -> crate::Result<Column> {
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
						.map(|(l_val, r_val)| l_val && r_val)
						.collect();

					Ok(Column {
						name: expr.full_fragment_owned(),
						data: ColumnData::bool(data),
					})
				} else {
					// Slow path: some values may be
					// undefined
					let mut data = Vec::with_capacity(l_container.data().len());
					let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

					for i in 0..l_container.data().len() {
						let l_val = l_container.get(i); // Option<bool>
						let r_val = r_container.get(i); // Option<bool>

						match (l_val, r_val) {
							(Some(false), _) | (_, Some(false)) => {
								// FALSE AND anything = FALSE
								data.push(false);
								bitvec.push(true);
							}
							(Some(true), Some(true)) => {
								// TRUE AND TRUE = TRUE
								data.push(true);
								bitvec.push(true);
							}
							_ => {
								// At least one is undefined and no FALSE values
								// undefined AND undefined = undefined, TRUE AND
								// undefined = undefined
								data.push(false);
								bitvec.push(false);
							}
						}
					}

					Ok(Column {
						name: expr.full_fragment_owned(),
						data: ColumnData::bool_with_bitvec(data, bitvec),
					})
				}
			}
			(ColumnData::Undefined(container), _) => Ok(Column {
				name: expr.full_fragment_owned(),
				data: ColumnData::Undefined(container.clone()),
			}),
			(_, ColumnData::Undefined(container)) => Ok(Column {
				name: expr.full_fragment_owned(),
				data: ColumnData::Undefined(container.clone()),
			}),
			(l, r) => {
				if l.is_number() || r.is_number() {
					return_error!(and_can_not_applied_to_number(expr.full_fragment_owned()));
				} else if l.is_text() || r.is_text() {
					return_error!(and_can_not_applied_to_text(expr.full_fragment_owned()));
				} else if l.is_temporal() || r.is_temporal() {
					return_error!(and_can_not_applied_to_temporal(expr.full_fragment_owned()));
				} else if l.is_uuid() || r.is_uuid() {
					return_error!(and_can_not_applied_to_uuid(expr.full_fragment_owned()));
				} else {
					unimplemented!("{} and {}", l.get_type(), r.get_type());
				}
			}
		}
	}
}
