// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::series::SeriesColumnToCreate;
use reifydb_core::interface::catalog::series::TimestampPrecision;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::ast::{AstColumnProperty, AstCreateSeries, AstTimestampPrecision},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateSeriesNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_series(
		&self,
		ast: AstCreateSeries<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns: Vec<SeriesColumnToCreate> = vec![];

		let series_namespace_name = ast.series.namespace.first().map(|n| n.text()).unwrap_or("default");

		for col in ast.columns.into_iter() {
			let constraint = convert_data_type_with_constraints(&col.ty)?;

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			let mut auto_increment = false;
			let mut dictionary_id = None;
			let properties = vec![];

			for property in &col.properties {
				match property {
					AstColumnProperty::AutoIncrement => auto_increment = true,
					AstColumnProperty::Dictionary(dict_ident) => {
						let dict_namespace_name = dict_ident
							.namespace
							.first()
							.map(|n| n.text())
							.unwrap_or(series_namespace_name);
						let dict_name = dict_ident.name.text();

						let Some(namespace) =
							self.catalog.find_namespace_by_name(tx, dict_namespace_name)?
						else {
							return Err(reifydb_catalog::error::CatalogError::NotFound {
								kind: reifydb_catalog::error::CatalogObjectKind::Dictionary,
								namespace: dict_namespace_name.to_string(),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id,
							dict_name,
						)?
						else {
							return Err(reifydb_catalog::error::CatalogError::NotFound {
								kind: reifydb_catalog::error::CatalogObjectKind::Dictionary,
								namespace: dict_namespace_name.to_string(),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						dictionary_id = Some(dictionary.id);
					}
					AstColumnProperty::Saturation(_) => {}
					AstColumnProperty::Default(_) => {}
				}
			}

			columns.push(SeriesColumnToCreate {
				name,
				fragment,
				constraint,
				properties,
				auto_increment,
				dictionary_id,
			});
		}

		let precision = match ast.precision {
			Some(AstTimestampPrecision::Millisecond) => TimestampPrecision::Millisecond,
			Some(AstTimestampPrecision::Microsecond) => TimestampPrecision::Microsecond,
			Some(AstTimestampPrecision::Nanosecond) => TimestampPrecision::Nanosecond,
			None => TimestampPrecision::Millisecond,
		};

		Ok(LogicalPlan::CreateSeries(CreateSeriesNode {
			series: ast.series,
			columns,
			tag: ast.tag,
			precision,
		}))
	}
}
