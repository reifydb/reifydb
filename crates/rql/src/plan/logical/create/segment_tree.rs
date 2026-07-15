// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::segment_tree::SegmentTreeColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::interface::catalog::{
	key::{KeySpec, TimestampPrecision},
	segment_tree::SegmentTreeAggregate,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::{AstErrorKind, TypeError},
	fragment::Fragment,
	value::value_type::ValueType,
};

use crate::{
	Result,
	ast::ast::{AstColumnProperty, AstCreateSegmentTree, AstTimestampPrecision},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateSegmentTreeNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_segment_tree(
		&self,
		ast: AstCreateSegmentTree<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let mut columns: Vec<SegmentTreeColumnToCreate> = vec![];

		let segment_tree_ns_segments: Vec<&str> = ast.segment_tree.namespace.iter().map(|n| n.text()).collect();

		for col in ast.columns.into_iter() {
			let constraint = convert_data_type_with_constraints(&col.ty)?;
			let column_type = constraint.get_type();

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
							segment_tree_ns_segments.clone()
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

						if column_type.inner_type() != &dictionary.value_type {
							return Err(CatalogError::DictionaryTypeMismatch {
								column: col.name.text().to_string(),
								column_type,
								dictionary: dict_name.to_string(),
								dictionary_value_type: dictionary.value_type,
								fragment: col.name.to_owned(),
							}
							.into());
						}

						dictionary_id = Some(dictionary.id);
					}
					AstColumnProperty::Saturation(_) => {}
					AstColumnProperty::Default(_) => {}
				}
			}

			columns.push(SegmentTreeColumnToCreate {
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
			ValueType::DateTime => {
				let precision = match ast.precision {
					Some(AstTimestampPrecision::Second) => TimestampPrecision::Second,
					Some(AstTimestampPrecision::Millisecond) => TimestampPrecision::Millisecond,
					Some(AstTimestampPrecision::Microsecond) => TimestampPrecision::Microsecond,
					Some(AstTimestampPrecision::Nanosecond) => TimestampPrecision::Nanosecond,
					None => TimestampPrecision::Millisecond,
				};
				KeySpec::DateTime {
					column: key_column_name,
					precision,
				}
			}
			ValueType::Int1
			| ValueType::Int2
			| ValueType::Int4
			| ValueType::Int8
			| ValueType::Int16
			| ValueType::Uint1
			| ValueType::Uint2
			| ValueType::Uint4
			| ValueType::Uint8
			| ValueType::Uint16 => {
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
				KeySpec::Integer {
					column: key_column_name,
				}
			}
			_ => {
				return Err(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "datetime or integer type for key column".to_string(),
					},
					message: format!(
						"key column '{}' has type {:?}, but only datetime and integer types are allowed as segment tree keys",
						key_column_name, key_type
					),
					fragment: key_fragment.to_owned(),
				}
				.into());
			}
		};

		if ast.aggregates.is_empty() {
			return Err(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "at least one aggregate".to_string(),
				},
				message: "CREATE SEGMENTTREE requires at least one aggregate".to_string(),
				fragment: key_fragment.to_owned(),
			}
			.into());
		}

		let mut aggregates: Vec<SegmentTreeAggregate> = Vec::with_capacity(ast.aggregates.len());
		for aggregate in &ast.aggregates {
			let alias_name = aggregate.alias.text().to_string();
			if aggregates.iter().any(|a| a.name == alias_name) {
				return Err(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "a unique aggregate alias".to_string(),
					},
					message: format!("aggregate alias '{}' is declared more than once", alias_name),
					fragment: aggregate.alias.to_owned(),
				}
				.into());
			}

			let aggregate_column_name = aggregate.column.text().to_string();
			if !columns.iter().any(|c| c.name.text() == aggregate_column_name.as_str()) {
				return Err(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: format!(
							"aggregate column '{}' to be a declared column",
							aggregate_column_name
						),
					},
					message: format!(
						"aggregate column '{}' is not declared in the column list",
						aggregate_column_name
					),
					fragment: aggregate.column.to_owned(),
				}
				.into());
			}

			let monoid = aggregate.function.iter().map(|f| f.text()).collect::<Vec<_>>().join("::");

			aggregates.push(SegmentTreeAggregate {
				name: alias_name,
				monoid,
				column: aggregate_column_name,
			});
		}

		let partition_by: Vec<String> = ast.partition_by.iter().map(|s| s.to_string()).collect();
		for pb_col in &partition_by {
			if !columns.iter().any(|c| c.name.text() == pb_col.as_str()) {
				return Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Column,
					namespace: segment_tree_ns_segments.join("::"),
					name: pb_col.clone(),
					fragment: Fragment::internal(pb_col.as_str()),
				}
				.into());
			}
		}

		if let Some(settings) = &ast.settings
			&& let Some(ttl) = &settings.ttl
		{
			return Err(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "no 'ttl' in row config".to_string(),
				},
				message: "row ttl is not supported on segment tree".to_string(),
				fragment: ttl.duration.fragment.to_owned(),
			}
			.into());
		}

		let persistent = match ast.settings {
			Some(settings) => settings.persistent.is_none_or(|p| p.value),
			None => true,
		};

		Ok(LogicalPlan::CreateSegmentTree(CreateSegmentTreeNode {
			segment_tree: ast.segment_tree,
			columns,
			key,
			aggregates,
			partition_by,
			persistent,
		}))
	}
}
