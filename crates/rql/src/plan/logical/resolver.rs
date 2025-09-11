// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{cell::RefCell, collections::HashMap};

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{
	Result,
	interface::identifier::{
		ColumnIdentifier, ColumnSource, FunctionIdentifier,
		IndexIdentifier, SchemaIdentifier, SequenceIdentifier,
		SourceIdentifier, SourceKind,
	},
};
use reifydb_type::{Fragment, IntoFragment, OwnedFragment};

use crate::{
	ast::identifier::{
		MaybeQualifiedColumnIdentifier, MaybeQualifiedColumnSource,
		MaybeQualifiedFunctionIdentifier,
		MaybeQualifiedIndexIdentifier, MaybeQualifiedSchemaIdentifier,
		MaybeQualifiedSequenceIdentifier,
		MaybeQualifiedSourceIdentifier,
	},
	error::{FunctionNotFoundError, IdentifierError, UnknownAliasError},
};

/// Context for resolving identifiers during logical planning
pub struct IdentifierResolver<'t, T: CatalogQueryTransaction> {
	default_schema: &'static str,
	source_aliases: RefCell<HashMap<String, SourceIdentifier<'static>>>,
	available_columns:
		RefCell<HashMap<(String, String), ColumnIdentifier<'static>>>,
	transaction: &'t mut T,
}

impl<'t, T: CatalogQueryTransaction> IdentifierResolver<'t, T> {
	pub fn new(
		transaction: &'t mut T,
		default_schema: &'static str,
	) -> Self {
		Self {
			default_schema,
			source_aliases: RefCell::new(HashMap::new()),
			available_columns: RefCell::new(HashMap::new()),
			transaction,
		}
	}

	/// Get the default schema
	pub fn default_schema(&self) -> &'static str {
		self.default_schema
	}

	/// Register an alias for a source
	pub fn register_alias(
		&self,
		alias: String,
		source: SourceIdentifier<'static>,
	) {
		self.source_aliases.borrow_mut().insert(alias, source);
	}

	/// Clear all aliases (useful when starting a new query scope)
	pub fn clear_aliases(&self) {
		self.source_aliases.borrow_mut().clear();
		self.available_columns.borrow_mut().clear();
	}

	/// Resolve a schema identifier
	pub fn resolve_schema(
		&mut self,
		schema: &SchemaIdentifier<'_>,
	) -> Result<SchemaIdentifier<'static>> {
		let schema_name = schema.name.text();

		// Validate schema exists
		self.transaction.get_schema_by_name(schema_name)?;

		Ok(SchemaIdentifier {
			name: Fragment::Owned(schema.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified schema to fully qualified
	pub fn resolve_maybe_schema<'a>(
		&mut self,
		schema: &MaybeQualifiedSchemaIdentifier<'a>,
	) -> Result<SchemaIdentifier<'static>> {
		let schema_id = SchemaIdentifier {
			name: schema.name.clone(),
		};
		self.resolve_schema(&schema_id)
	}

	/// Resolve a source identifier to fully qualified form
	pub fn resolve_source(
		&mut self,
		source: &SourceIdentifier<'_>,
	) -> Result<SourceIdentifier<'static>> {
		// Validate the schema exists (schema is always present in fully
		// qualified identifiers)
		let schema_name = source.schema.text();

		let _schema = self.transaction.get_schema_by_name(schema_name);

		let resolved_schema =
			Fragment::Owned(source.schema.clone().into_owned());

		// Determine source type from catalog
		let source_kind = self.determine_source_kind(
			Some(resolved_schema.text()),
			source.name.clone(),
		)?;

		let mut result = SourceIdentifier::new(
			resolved_schema,
			Fragment::Owned(source.name.clone().into_owned()),
			source_kind,
		);

		if let Some(alias) = &source.alias {
			result = result.with_alias(Fragment::Owned(
				alias.clone().into_owned(),
			));
		}

		Ok(result)
	}

	/// Convert and resolve a maybe-qualified source to fully qualified
	pub fn resolve_maybe_source<'a>(
		&mut self,
		source: &MaybeQualifiedSourceIdentifier<'a>,
	) -> Result<SourceIdentifier<'static>> {
		self.resolve_maybe_source_with_validation(source, true)
	}

	/// Convert and resolve a maybe-qualified source to fully qualified with
	/// optional validation
	pub fn resolve_maybe_source_with_validation<'a>(
		&mut self,
		source: &MaybeQualifiedSourceIdentifier<'a>,
		validate_existence: bool,
	) -> Result<SourceIdentifier<'static>> {
		// Determine schema to use
		let resolved_schema = match &source.schema {
			Some(schema) => {
				// User provided explicit schema - validate it
				// exists
				self.transaction.get_schema_by_name(schema)?;
				Fragment::Owned(schema.clone().into_owned())
			}
			None => {
				// No schema provided - use default schema
				// Use Internal fragment type to indicate this
				// was injected
				Fragment::Owned(OwnedFragment::Internal {
					text: self.default_schema.to_string(),
				})
			}
		};

		// Now create a fully qualified SourceIdentifier
		let mut full_source = SourceIdentifier::new(
			resolved_schema,
			Fragment::Owned(source.name.clone().into_owned()),
			source.kind,
		);

		if let Some(alias) = &source.alias {
			full_source = full_source.with_alias(Fragment::Owned(
				alias.clone().into_owned(),
			));
		}

		// If validation is disabled, return the resolved identifier
		// without checking existence
		if !validate_existence {
			return Ok(full_source);
		}

		// Otherwise, perform normal validation
		self.resolve_source(&full_source)
	}

	/// Resolve a column identifier to fully qualified form
	pub fn resolve_column(
		&mut self,
		column: &ColumnIdentifier<'_>,
	) -> Result<ColumnIdentifier<'static>> {
		let resolved_source = match &column.source {
			ColumnSource::Source {
				schema,
				source,
			} => {
				// Column is already fully qualified - just
				// validate it exists
				let schema_name = schema.text();

				// Validate source exists
				self.validate_source_exists(
					Some(schema_name),
					source.text(),
				)?;

				ColumnSource::Source {
					schema: Fragment::Owned(
						schema.clone().into_owned(),
					),
					source: Fragment::Owned(
						source.clone().into_owned(),
					),
				}
			}
			ColumnSource::Alias(alias) => {
				// Column qualified by alias - check it exists
				if !self.source_aliases
					.borrow()
					.contains_key(alias.text())
				{
					return Err(
						IdentifierError::UnknownAlias(
							UnknownAliasError {
								alias: alias
									.text()
									.to_string(
									),
							},
						)
						.into(),
					);
				}
				ColumnSource::Alias(Fragment::Owned(
					alias.clone().into_owned(),
				))
			}
		};

		Ok(ColumnIdentifier {
			source: resolved_source,
			name: Fragment::Owned(column.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified column to fully qualified
	pub fn resolve_maybe_column<'a>(
		&mut self,
		column: &MaybeQualifiedColumnIdentifier<'a>,
	) -> Result<ColumnIdentifier<'static>> {
		let resolved_source = match &column.source {
			MaybeQualifiedColumnSource::Source {
				schema,
				source,
			} => {
				// Column qualified by source name
				let resolved_schema = match schema {
					Some(s) => {
						// Validate schema exists
						let schema_name = s.text();
						self.transaction
							.get_schema_by_name(
								schema_name,
							)?;
						Fragment::Owned(
							s.clone().into_owned(),
						)
					}
					None => {
						// Inject default schema
						Fragment::Owned(OwnedFragment::Internal {
                            text: self.default_schema.to_string(),
                        })
					}
				};

				// Validate source exists
				self.validate_source_exists(
					Some(resolved_schema.text()),
					source.text(),
				)?;

				ColumnSource::Source {
					schema: resolved_schema,
					source: Fragment::Owned(
						source.clone().into_owned(),
					),
				}
			}
			MaybeQualifiedColumnSource::Alias(alias) => {
				// Column qualified by alias - check it
				// exists
				if !self.source_aliases
					.borrow()
					.contains_key(alias.text())
				{
					return Err(
						IdentifierError::UnknownAlias(
							UnknownAliasError {
								alias: alias
									.text()
									.to_string(
									),
							},
						)
						.into(),
					);
				}
				ColumnSource::Alias(Fragment::Owned(
					alias.clone().into_owned(),
				))
			}
			MaybeQualifiedColumnSource::Unqualified => {
				// Unqualified column - need to find
				// which source it belongs to
				// For now, we'll create an unqualified
				// column that will need resolution
				// based on context
				// In a real implementation, we'd look
				// up available columns
				let column_name = column.name.text();
				let matching_sources =
					self.find_column_sources(column_name);

				match matching_sources.len() {
					0 => {
						// Column not found -
						// for now, create
						// unqualified
						// The actual error will
						// be caught during
						// execution
						return Err(IdentifierError::ColumnNotFound {
                            column: column_name.to_string(),
                        }.into());
					}
					1 => {
						// Unambiguous - qualify
						// with the single
						// source
						let (_, source_id) =
							matching_sources
								.into_iter()
								.next()
								.unwrap();
						ColumnSource::Source {
							schema: source_id
								.schema,
							source: source_id.name,
						}
					}
					_ => {
						// Ambiguous - report
						// error
						let sources: Vec<String> = matching_sources
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect();
						return Err(IdentifierError::AmbiguousColumn(
                            crate::error::AmbiguousColumnError {
                                column: column_name.to_string(),
                                sources,
                            }
                        ).into());
					}
				}
			}
		};

		Ok(ColumnIdentifier {
			source: resolved_source,
			name: Fragment::Owned(column.name.clone().into_owned()),
		})
	}

	/// Resolve a function identifier
	pub fn resolve_function(
		&self,
		func: &FunctionIdentifier<'_>,
	) -> Result<FunctionIdentifier<'static>> {
		// Validate function exists in catalog
		let namespaces: Vec<String> = func
			.namespaces
			.iter()
			.map(|f| f.text().to_string())
			.collect();
		let function_name = func.name.text();

		// TODO: Validate function exists once CatalogQueryTransaction
		// supports it
		if false {
			return Err(IdentifierError::FunctionNotFound(
				FunctionNotFoundError {
					namespaces,
					name: function_name.to_string(),
				},
			)
			.into());
		}

		Ok(FunctionIdentifier {
			namespaces: func
				.namespaces
				.iter()
				.map(|f| {
					Fragment::Owned(f.clone().into_owned())
				})
				.collect(),
			name: Fragment::Owned(func.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified function to fully qualified
	pub fn resolve_maybe_function<'a>(
		&self,
		func: &MaybeQualifiedFunctionIdentifier<'a>,
	) -> Result<FunctionIdentifier<'static>> {
		// Create fully qualified function identifier
		let full_func = FunctionIdentifier {
			namespaces: func.namespaces.clone(),
			name: func.name.clone(),
		};
		self.resolve_function(&full_func)
	}

	/// Resolve a sequence identifier
	pub fn resolve_sequence(
		&self,
		seq: &SequenceIdentifier<'_>,
	) -> Result<SequenceIdentifier<'static>> {
		// Validate sequence exists
		let schema_name = seq.schema.text();
		let seq_name = seq.name.text();

		// TODO: Add sequence validation once CatalogQueryTransaction
		// supports it
		if false {
			return Err(IdentifierError::SequenceNotFound {
				schema: schema_name.to_string(),
				name: seq_name.to_string(),
			}
			.into());
		}

		Ok(SequenceIdentifier {
			schema: Fragment::Owned(
				seq.schema.clone().into_owned(),
			),
			name: Fragment::Owned(seq.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified sequence to fully qualified
	pub fn resolve_maybe_sequence<'a>(
		&mut self,
		seq: &MaybeQualifiedSequenceIdentifier<'a>,
	) -> Result<SequenceIdentifier<'static>> {
		// Determine schema to use
		let resolved_schema = match &seq.schema {
			Some(schema) => {
				// Validate schema exists
				self.transaction.get_schema_by_name(schema)?;
				Fragment::Owned(schema.clone().into_owned())
			}
			None => {
				// Inject default schema
				let default_schema = self.default_schema;
				Fragment::Owned(OwnedFragment::Internal {
					text: default_schema.to_string(),
				})
			}
		};

		// Create fully qualified sequence identifier
		let full_seq = SequenceIdentifier {
			schema: resolved_schema,
			name: Fragment::Owned(seq.name.clone().into_owned()),
		};
		self.resolve_sequence(&full_seq)
	}

	/// Resolve an index identifier
	pub fn resolve_index(
		&self,
		idx: &IndexIdentifier<'_>,
	) -> Result<IndexIdentifier<'static>> {
		// Validate index exists
		let schema_name = idx.schema.text();
		let table_name = idx.table.text();
		let index_name = idx.name.text();

		// TODO: Add index validation once CatalogQueryTransaction
		// supports it
		if false {
			return Err(IdentifierError::IndexNotFound {
				schema: schema_name.to_string(),
				table: table_name.to_string(),
				name: index_name.to_string(),
			}
			.into());
		}

		Ok(IndexIdentifier {
			schema: Fragment::Owned(
				idx.schema.clone().into_owned(),
			),
			table: Fragment::Owned(idx.table.clone().into_owned()),
			name: Fragment::Owned(idx.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified index to fully qualified
	pub fn resolve_maybe_index<'a>(
		&mut self,
		idx: &MaybeQualifiedIndexIdentifier<'a>,
	) -> Result<IndexIdentifier<'static>> {
		// Determine schema to use
		let resolved_schema = match &idx.schema {
			Some(schema) => {
				self.transaction.get_schema_by_name(schema)?;
				Fragment::Owned(schema.clone().into_owned())
			}
			None => {
				// Inject default schema
				let default_schema = self.default_schema;
				Fragment::Owned(OwnedFragment::Internal {
					text: default_schema.to_string(),
				})
			}
		};

		// Create fully qualified index identifier
		let full_idx = IndexIdentifier {
			schema: resolved_schema,
			table: Fragment::Owned(idx.table.clone().into_owned()),
			name: Fragment::Owned(idx.name.clone().into_owned()),
		};
		self.resolve_index(&full_idx)
	}

	// Helper methods

	fn determine_source_kind<'a>(
		&mut self,
		schema: Option<&str>,
		name: impl IntoFragment<'a>,
	) -> Result<SourceKind> {
		let default_schema = self.default_schema;
		let schema = schema.unwrap_or_else(|| &*default_schema);

		// Check catalog for source type
		// First, get the schema ID
		let schema = self.transaction.get_schema_by_name(schema)?;
		// let source = self.transaction.get_source_by_name(schema.id,
		// name)?;

		let name = name.into_fragment();

		// FIXME todo
		if self.transaction
			.find_table_by_name(schema.id, name.text())?
			.is_some()
		{
			return Ok(SourceKind::Table);
		}

		if self.transaction
			.find_view_by_name(schema.id, name.text())?
			.is_some()
		{
			return Ok(SourceKind::View);
		}

		// Ok(match source {
		// 	SourceDef::Table(_) => SourceKind::Table,
		// 	SourceDef::View(_) => SourceKind::View,
		// 	SourceDef::TableVirtual(_) => SourceKind::TableVirtual,
		// })

		// FIXME
		return Ok(SourceKind::TableVirtual);
	}

	#[allow(dead_code)]
	fn resolve_schema_fragment(
		&mut self,
		schema: &Option<Fragment<'_>>,
	) -> Result<Option<Fragment<'static>>> {
		Ok(match schema {
			Some(s) => {
				self.transaction.get_schema_by_name(s)?;
				Some(Fragment::Owned(s.clone().into_owned()))
			}
			None => {
				// Inject default schema if available
				let default_schema = self.default_schema;
				Some(Fragment::Owned(OwnedFragment::Internal {
					text: default_schema.to_string(),
				}))
			}
		})
	}

	fn validate_source_exists(
		&mut self,
		schema: Option<&str>,
		name: &str,
	) -> Result<()> {
		// Validate source exists using determine_source_kind
		self.determine_source_kind(schema, name)?;
		Ok(())
	}

	#[allow(dead_code)]
	fn find_column_sources(
		&self,
		column_name: &str,
	) -> Vec<(String, SourceIdentifier<'static>)> {
		let mut sources = Vec::new();

		// Check all registered aliases
		let aliases = self.source_aliases.borrow();
		for (alias, source) in aliases.iter() {
			if self.source_has_column(source, column_name) {
				sources.push((alias.clone(), source.clone()));
			}
		}

		sources
	}

	#[allow(dead_code)]
	fn source_has_column(
		&self,
		source: &SourceIdentifier<'static>,
		_column_name: &str,
	) -> bool {
		let _schema = source.schema.text();
		let _source_name = source.name.text();

		match source.kind {
			SourceKind::Table => {
				// TODO: Check table has column once
				// CatalogQueryTransaction supports it
				true
			}
			SourceKind::View
			| SourceKind::MaterializedView
			| SourceKind::DeferredView
			| SourceKind::TransactionalView => {
				// TODO: Check view has column once
				// CatalogQueryTransaction supports it
				true
			}
			SourceKind::CTE => {
				// For CTEs, we'd need to track their output
				// columns This would be done during CTE
				// registration For now, return false
				false
			}
			_ => false,
		}
	}

	/// Register available columns for a source
	/// This should be called after resolving a source in FROM/JOIN
	pub fn register_source_columns(
		&mut self,
		source: &SourceIdentifier<'static>,
	) -> Result<()> {
		let _schema = source.schema.text();
		let _source_name = source.name.text();
		let effective_name = source.effective_name();

		// Get columns based on source type
		use reifydb_core::interface::ColumnDef;
		let columns: Vec<ColumnDef> = match source.kind {
			SourceKind::Table => {
				// TODO: Get table columns once
				// CatalogQueryTransaction supports it
				Vec::new()
			}
			SourceKind::View
			| SourceKind::MaterializedView
			| SourceKind::DeferredView
			| SourceKind::TransactionalView => {
				// TODO: Get view columns once
				// CatalogQueryTransaction supports it
				Vec::new()
			}
			_ => Vec::new(),
		};

		// Register each column as available
		for column in columns {
			let col_ident = ColumnIdentifier {
				source: if source.alias.is_some() {
					ColumnSource::Alias(
						source.alias.clone().unwrap(),
					)
				} else {
					ColumnSource::Source {
						schema: source.schema.clone(),
						source: source.name.clone(),
					}
				},
				name: Fragment::Owned(
					OwnedFragment::Internal {
						text: column.name.clone(),
					},
				),
			};

			self.available_columns.borrow_mut().insert(
				(effective_name.to_string(), column.name),
				col_ident,
			);
		}

		Ok(())
	}
}
