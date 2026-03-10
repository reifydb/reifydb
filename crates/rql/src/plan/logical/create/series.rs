// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	catalog::series::SeriesColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::interface::catalog::series::TimestampPrecision;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	ast::ast::{AstColumnProperty, AstCreateSeries, AstTimestampPrecision},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateSeriesNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_series(
		&self,
		ast: AstCreateSeries<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let mut columns: Vec<SeriesColumnToCreate> = vec![];

		let series_ns_segments: Vec<&str> = ast.series.namespace.iter().map(|n| n.text()).collect();

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
						let dict_ns_segments: Vec<&str> = if dict_ident.namespace.is_empty() {
							series_ns_segments.clone()
						} else {
							dict_ident.namespace.iter().map(|n| n.text()).collect()
						};
						let dict_name = dict_ident.name.text();

						let Some(namespace) = self
							.catalog
							.find_namespace_by_segments(tx, &dict_ns_segments)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_ns_segments.join("::"),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id(),
							dict_name,
						)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_ns_segments.join("::"),
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
