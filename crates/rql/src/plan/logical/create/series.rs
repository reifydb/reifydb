// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	catalog::series::SeriesColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::interface::catalog::series::{SeriesKey, TimestampPrecision};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::{AstErrorKind, TypeError},
	fragment::Fragment,
	value::r#type::Type,
};

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

		let key_fragment = ast.key.expect("key is required by parser");
		let key_column_name = key_fragment.text().to_string();

		let key_col = columns.iter().find(|c| c.name.text() == key_column_name.as_str());
		let key_col = match key_col {
			Some(c) => c,
			None => {
				return Err(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: format!(
							"key column '{}' to be a declared column",
							key_column_name
						),
					},
					message: format!(
						"key column '{}' is not declared in the column list",
						key_column_name
					),
					fragment: key_fragment.to_owned(),
				}
				.into());
			}
		};

		let key_type = key_col.constraint.get_type();
		let key = match key_type {
			Type::DateTime => {
				let precision = match ast.precision {
					Some(AstTimestampPrecision::Second) => TimestampPrecision::Second,
					Some(AstTimestampPrecision::Millisecond) => TimestampPrecision::Millisecond,
					Some(AstTimestampPrecision::Microsecond) => TimestampPrecision::Microsecond,
					Some(AstTimestampPrecision::Nanosecond) => TimestampPrecision::Nanosecond,
					None => TimestampPrecision::Millisecond,
				};
				SeriesKey::DateTime {
					column: key_column_name,
					precision,
				}
			}
			Type::Int1
			| Type::Int2
			| Type::Int4
			| Type::Int8
			| Type::Int16
			| Type::Uint1
			| Type::Uint2
			| Type::Uint4
			| Type::Uint8
			| Type::Uint16 => {
				if ast.precision.is_some() {
					return Err(TypeError::Ast {
						kind: AstErrorKind::UnexpectedToken {
							expected: "no precision for integer key columns".to_string(),
						},
						message: "precision can only be specified for datetime key columns"
							.to_string(),
						fragment: key_fragment.to_owned(),
					}
					.into());
				}
				SeriesKey::Integer {
					column: key_column_name,
				}
			}
			_ => {
				return Err(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "datetime or integer type for key column".to_string(),
					},
					message: format!(
						"key column '{}' has type {:?}, but only datetime and integer types are allowed as series keys",
						key_column_name, key_type
					),
					fragment: key_fragment.to_owned(),
				}
				.into());
			}
		};

		let ttl = ast.ttl.map(Self::compile_ttl).transpose()?;

		Ok(LogicalPlan::CreateSeries(CreateSeriesNode {
			series: ast.series,
			columns,
			tag: ast.tag,
			key,
			ttl,
		}))
	}
}
