// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	CommitVersion, Result,
	interface::{
		ViewKind,
		identifier::{
			ColumnIdentifier, ColumnSource, FunctionIdentifier,
			IndexIdentifier, SchemaIdentifier, SequenceIdentifier,
			SourceIdentifier, SourceKind,
		},
	},
};
use reifydb_type::{Fragment, OwnedFragment};

use crate::error::{
	FunctionNotFoundError, IdentifierError, SchemaNotFoundError,
	SourceNotFoundError, UnknownAliasError,
};

/// Context for resolving identifiers during logical planning
pub struct IdentifierResolver {
	/// User's default schema (from session/connection)
	default_schema: Option<String>,

	/// Maps aliases to fully qualified source identifiers
	/// Built up as FROM/JOIN clauses are processed
	aliases: HashMap<String, SourceIdentifier<'static>>,

	/// Available columns in current scope
	/// Maps (source_alias_or_name, column_name) -> ColumnIdentifier
	available_columns: HashMap<(String, String), ColumnIdentifier<'static>>,

	/// Stack of CTE definitions for WITH clauses
	cte_stack: Vec<HashMap<String, SourceIdentifier<'static>>>,

	/// Catalog for validation and type determination
	catalog: MaterializedCatalog,

	/// Current transaction version for catalog lookups
	version: CommitVersion,
}

impl IdentifierResolver {
	pub fn new(
		catalog: MaterializedCatalog,
		version: CommitVersion,
		default_schema: Option<String>,
	) -> Self {
		Self {
			default_schema,
			aliases: HashMap::new(),
			available_columns: HashMap::new(),
			cte_stack: Vec::new(),
			catalog,
			version,
		}
	}

	/// Get the default schema if set
	pub fn default_schema(&self) -> Option<&str> {
		self.default_schema.as_deref()
	}

	/// Set the default schema
	pub fn set_default_schema(&mut self, schema: Option<String>) {
		self.default_schema = schema;
	}

	/// Push a new CTE scope
	pub fn push_cte_scope(&mut self) {
		self.cte_stack.push(HashMap::new());
	}

	/// Pop CTE scope
	pub fn pop_cte_scope(&mut self) {
		self.cte_stack.pop();
	}

	/// Register a CTE
	pub fn register_cte(
		&mut self,
		name: String,
		source: SourceIdentifier<'static>,
	) {
		if let Some(scope) = self.cte_stack.last_mut() {
			scope.insert(name, source);
		}
	}

	/// Register an alias for a source
	pub fn register_alias(
		&mut self,
		alias: String,
		source: SourceIdentifier<'static>,
	) {
		self.aliases.insert(alias, source);
	}

	/// Clear all aliases (useful when starting a new query scope)
	pub fn clear_aliases(&mut self) {
		self.aliases.clear();
		self.available_columns.clear();
	}

	/// Resolve a schema identifier
	pub fn resolve_schema(
		&self,
		schema: &SchemaIdentifier<'_>,
	) -> Result<SchemaIdentifier<'static>> {
		let schema_name = schema.name.text();

		// Validate schema exists
		if !self.catalog.schema_exists(schema_name, self.version) {
			return Err(IdentifierError::SchemaNotFound(
				SchemaNotFoundError {
					schema: schema_name.to_string(),
				},
			)
			.into());
		}

		Ok(SchemaIdentifier {
			name: Fragment::Owned(schema.name.clone().into_owned()),
		})
	}

	/// Resolve a source identifier to fully qualified form
	pub fn resolve_source(
		&self,
		source: &SourceIdentifier<'_>,
	) -> Result<SourceIdentifier<'static>> {
		// First check if this references a CTE
		for cte_scope in self.cte_stack.iter().rev() {
			if let Some(cte_source) =
				cte_scope.get(source.name.text())
			{
				let mut resolved = cte_source.clone();
				// Preserve the alias from the original source
				if let Some(alias) = &source.alias {
					resolved.alias = Some(Fragment::Owned(
						alias.clone().into_owned(),
					));
				}
				return Ok(resolved);
			}
		}

		// Validate the schema exists (schema is always present in fully
		// qualified identifiers)
		let schema_name = source.schema.text();

		// Check if this is the injected default schema (Internal
		// fragment type) or if it was explicitly provided by the user
		let _is_default_schema = matches!(
			source.schema,
			Fragment::Owned(OwnedFragment::Internal { .. })
		);

		if !self.catalog.schema_exists(schema_name, self.version) {
			return Err(IdentifierError::SchemaNotFound(
				SchemaNotFoundError {
					schema: schema_name.to_string(),
				},
			)
			.into());
		}

		let resolved_schema =
			Fragment::Owned(source.schema.clone().into_owned());

		// Determine source type from catalog
		let source_kind = self.determine_source_kind(
			Some(resolved_schema.text()),
			source.name.text(),
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

	/// Resolve a column identifier to fully qualified form
	pub fn resolve_column(
		&self,
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
				if !self.aliases.contains_key(alias.text()) {
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

		if !self.catalog.function_exists(
			&namespaces,
			function_name,
			self.version,
		) {
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

	/// Resolve a sequence identifier
	pub fn resolve_sequence(
		&self,
		seq: &SequenceIdentifier<'_>,
	) -> Result<SequenceIdentifier<'static>> {
		// Validate sequence exists
		let schema_name = seq.schema.text();
		let seq_name = seq.name.text();

		if !self.catalog.sequence_exists(
			schema_name,
			seq_name,
			self.version,
		) {
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

	/// Resolve an index identifier
	pub fn resolve_index(
		&self,
		idx: &IndexIdentifier<'_>,
	) -> Result<IndexIdentifier<'static>> {
		// Validate index exists
		let schema_name = idx.schema.text();
		let table_name = idx.table.text();
		let index_name = idx.name.text();

		if !self.catalog.index_exists(
			schema_name,
			table_name,
			index_name,
			self.version,
		) {
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

	// Helper methods

	fn determine_source_kind(
		&self,
		schema: Option<&str>,
		name: &str,
	) -> Result<SourceKind> {
		let schema = schema.unwrap_or_else(|| {
			self.default_schema.as_deref().unwrap_or("public")
		});

		// Check catalog for source type
		if self.catalog.table_exists(schema, name, self.version) {
			Ok(SourceKind::Table)
		} else if self.catalog.view_exists(schema, name, self.version) {
			// Determine specific view type
			let view_def = self.catalog.get_view(
				schema,
				name,
				self.version,
			)?;
			Ok(match view_def.kind {
				ViewKind::Deferred => SourceKind::DeferredView,
				ViewKind::Transactional => {
					SourceKind::TransactionalView
				}
			})
		} else {
			Err(IdentifierError::SourceNotFound(
				SourceNotFoundError {
					schema: schema.to_string(),
					name: name.to_string(),
				},
			)
			.into())
		}
	}

	#[allow(dead_code)]
	fn resolve_schema_fragment(
		&self,
		schema: &Option<Fragment<'_>>,
	) -> Result<Option<Fragment<'static>>> {
		Ok(match schema {
			Some(s) => {
				// Validate schema exists
				let schema_name = s.text();
				if !self.catalog.schema_exists(
					schema_name,
					self.version,
				) {
					return Err(IdentifierError::SchemaNotFound(
						SchemaNotFoundError {
							schema: schema_name.to_string(),
						},
					)
					.into());
				}
				Some(Fragment::Owned(s.clone().into_owned()))
			}
			None => {
				// Inject default schema if available
				self.default_schema.as_ref().map(|default| {
					Fragment::Owned(
						OwnedFragment::Internal {
							text: default.clone(),
						},
					)
				})
			}
		})
	}

	fn validate_source_exists(
		&self,
		schema: Option<&str>,
		name: &str,
	) -> Result<()> {
		let schema = schema.unwrap_or_else(|| {
			self.default_schema.as_deref().unwrap_or("public")
		});

		if !self.catalog.source_exists(schema, name, self.version) {
			return Err(IdentifierError::SourceNotFound(
				SourceNotFoundError {
					schema: schema.to_string(),
					name: name.to_string(),
				},
			)
			.into());
		}

		Ok(())
	}

	#[allow(dead_code)]
	fn find_column_sources(
		&self,
		column_name: &str,
	) -> Vec<(String, &SourceIdentifier<'static>)> {
		let mut sources = Vec::new();

		// Check all registered aliases
		for (alias, source) in &self.aliases {
			if self.source_has_column(source, column_name) {
				sources.push((alias.clone(), source));
			}
		}

		// Check CTEs
		for cte_scope in &self.cte_stack {
			for (name, source) in cte_scope {
				if self.source_has_column(source, column_name) {
					sources.push((name.clone(), source));
				}
			}
		}

		sources
	}

	#[allow(dead_code)]
	fn source_has_column(
		&self,
		source: &SourceIdentifier<'static>,
		column_name: &str,
	) -> bool {
		let schema = source.schema.text();
		let source_name = source.name.text();

		match source.kind {
			SourceKind::Table => self.catalog.table_has_column(
				schema,
				source_name,
				column_name,
				self.version,
			),
			SourceKind::View
			| SourceKind::MaterializedView
			| SourceKind::DeferredView
			| SourceKind::TransactionalView => self.catalog.view_has_column(
				schema,
				source_name,
				column_name,
				self.version,
			),
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
		let schema = source.schema.text();
		let source_name = source.name.text();
		let effective_name = source.effective_name();

		// Get columns based on source type
		let columns = match source.kind {
			SourceKind::Table => self.catalog.get_table_columns(
				schema,
				source_name,
				self.version,
			)?,
			SourceKind::View
			| SourceKind::MaterializedView
			| SourceKind::DeferredView
			| SourceKind::TransactionalView => self.catalog.get_view_columns(
				schema,
				source_name,
				self.version,
			)?,
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

			self.available_columns.insert(
				(effective_name.to_string(), column.name),
				col_ident,
			);
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn create_test_resolver() -> IdentifierResolver {
		let catalog = MaterializedCatalog::new();
		let version: CommitVersion = 1;
		IdentifierResolver::new(
			catalog,
			version,
			Some("public".to_string()),
		)
	}

	#[test]
	fn test_default_schema_injection() {
		let resolver = create_test_resolver();
		assert_eq!(resolver.default_schema(), Some("public"));
	}

	#[test]
	fn test_alias_registration() {
		let mut resolver = create_test_resolver();
		let source = SourceIdentifier::new(
			Fragment::Owned(OwnedFragment::Internal {
				text: "public".to_string(),
			}),
			Fragment::Owned(OwnedFragment::Internal {
				text: "users".to_string(),
			}),
			SourceKind::Table,
		);

		resolver.register_alias("u".to_string(), source);
		assert!(resolver.aliases.contains_key("u"));
	}

	#[test]
	fn test_cte_scope() {
		let mut resolver = create_test_resolver();
		resolver.push_cte_scope();

		let source = SourceIdentifier::new(
			Fragment::Owned(OwnedFragment::Internal {
				text: "public".to_string(),
			}),
			Fragment::Owned(OwnedFragment::Internal {
				text: "temp_result".to_string(),
			}),
			SourceKind::CTE,
		);

		resolver.register_cte("temp".to_string(), source);
		assert_eq!(resolver.cte_stack.len(), 1);

		resolver.pop_cte_scope();
		assert_eq!(resolver.cte_stack.len(), 0);
	}

	#[test]
	fn test_clear_aliases() {
		let mut resolver = create_test_resolver();
		let source = SourceIdentifier::new(
			Fragment::Owned(OwnedFragment::Internal {
				text: "public".to_string(),
			}),
			Fragment::Owned(OwnedFragment::Internal {
				text: "users".to_string(),
			}),
			SourceKind::Table,
		);

		resolver.register_alias("u".to_string(), source);
		assert!(!resolver.aliases.is_empty());

		resolver.clear_aliases();
		assert!(resolver.aliases.is_empty());
		assert!(resolver.available_columns.is_empty());
	}
}
