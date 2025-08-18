// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::OrExpression},
	result::error::diagnostic::operator::{
		or_can_not_applied_to_number, or_can_not_applied_to_temporal,
		or_can_not_applied_to_text, or_can_not_applied_to_uuid,
	},
	return_error,
};

use crate::{
	columnar::{Column, ColumnData, ColumnQualified},
	evaluate::{EvaluationContext, StandardEvaluator},
};

impl StandardEvaluator {
	pub(crate) fn or(
		&self,
		ctx: &EvaluationContext,
		expr: &OrExpression,
	) -> crate::Result<Column> {
		let left = self.evaluate(ctx, &expr.left)?;
		let right = self.evaluate(ctx, &expr.right)?;

		match (&left.data(), &right.data()) {
			(
				ColumnData::Bool(l_container),
				ColumnData::Bool(r_container),
			) => {
				let mut data = Vec::with_capacity(
					l_container.data().len(),
				);
				let mut bitvec = Vec::with_capacity(
					l_container.bitvec().len(),
				);

				for i in 0..l_container.data().len() {
					if l_container.is_defined(i)
						&& r_container.is_defined(i)
					{
						data.push(l_container
							.data()
							.get(i)
							|| r_container
								.data()
								.get(i));
						bitvec.push(true);
					} else {
						data.push(false);
						bitvec.push(false);
					}
				}

				Ok(Column::ColumnQualified(ColumnQualified {
					name: expr.fragment().fragment().into(),
					data: ColumnData::bool_with_bitvec(
						data, bitvec,
					),
				}))
			}
			(l, r) => {
				if l.is_number() || r.is_number() {
					return_error!(
						or_can_not_applied_to_number(
							expr.fragment()
						)
					);
				} else if l.is_text() || r.is_text() {
					return_error!(
						or_can_not_applied_to_text(
							expr.fragment()
						)
					);
				} else if l.is_temporal() || r.is_temporal() {
					return_error!(
						or_can_not_applied_to_temporal(
							expr.fragment()
						)
					);
				} else {
					return_error!(
						or_can_not_applied_to_uuid(
							expr.fragment()
						)
					);
				}
			}
		}
	}
}
