// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::resolved::ResolvedObject;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::constraint::Constraint;

use crate::{
	Result,
	expression::{Expression, name::display_label},
};

pub fn resolve_is_variant_tags(
	expr: &mut Expression,
	source: &ResolvedObject,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
) -> Result<()> {
	match expr {
		Expression::IsVariant(e) => {
			let col_name = match e.expression.as_ref() {
				Expression::Column(c) => c.0.name.text().to_string(),
				other => display_label(other).text().to_string(),
			};

			let tag_col_name = format!("{}_tag", col_name);
			let columns = source.columns();
			if let Some(tag_col) = columns.iter().find(|c| c.name == tag_col_name)
				&& let Some(Constraint::SumType(id)) = tag_col.constraint.constraint()
			{
				let def = catalog.get_sumtype(rx, *id)?;
				let variant_name = e.variant_name.text().to_lowercase();
				if let Some(variant) =
					def.variants.iter().find(|v| v.name.to_lowercase() == variant_name)
				{
					e.tag = Some(variant.tag);
				}
			}
			resolve_is_variant_tags(&mut e.expression, source, catalog, rx)?;
		}
		Expression::And(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::Or(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::Equal(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::NotEqual(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::Prefix(e) => {
			resolve_is_variant_tags(&mut e.expression, source, catalog, rx)?;
		}
		Expression::If(e) => {
			resolve_is_variant_tags(&mut e.condition, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.then_expr, source, catalog, rx)?;
			for else_if in &mut e.else_ifs {
				resolve_is_variant_tags(&mut else_if.condition, source, catalog, rx)?;
				resolve_is_variant_tags(&mut else_if.then_expr, source, catalog, rx)?;
			}
			if let Some(else_expr) = &mut e.else_expr {
				resolve_is_variant_tags(else_expr, source, catalog, rx)?;
			}
		}
		Expression::Alias(e) => {
			resolve_is_variant_tags(&mut e.expression, source, catalog, rx)?;
		}
		_ => {}
	}
	Ok(())
}
