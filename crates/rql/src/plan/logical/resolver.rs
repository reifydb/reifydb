// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use reifydb_catalog::{CatalogQueryTransaction, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{
		TableVirtualDef, ViewKind,
		identifier::{
			ColumnIdentifier, ColumnSource, DeferredViewIdentifier, FunctionIdentifier, IndexIdentifier,
			NamespaceIdentifier, RingBufferIdentifier, SequenceIdentifier, SourceIdentifier,
			TableIdentifier, TableVirtualIdentifier, TransactionalViewIdentifier,
			UnresolvedSourceIdentifier,
		},
		resolved::{
			ResolvedColumn, ResolvedDeferredView, ResolvedNamespace, ResolvedSource, ResolvedTable,
			ResolvedTableVirtual, ResolvedTransactionalView, ResolvedView,
		},
	},
};
use reifydb_type::{Fragment, IntoFragment, OwnedFragment};

use crate::{
	ast::identifier::{
		MaybeQualifiedColumnIdentifier, MaybeQualifiedColumnSource, MaybeQualifiedDeferredViewIdentifier,
		MaybeQualifiedFunctionIdentifier, MaybeQualifiedIndexIdentifier, MaybeQualifiedNamespaceIdentifier,
		MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSequenceIdentifier, MaybeQualifiedTableIdentifier,
		MaybeQualifiedTransactionalViewIdentifier, MaybeQualifiedViewIdentifier,
	},
	error::{FunctionNotFoundError, IdentifierError, UnknownAliasError},
};

/// Context for resolving identifiers during logical planning
pub struct IdentifierResolver<'t, T: CatalogQueryTransaction> {
	default_namespace: &'static str,
	source_aliases: RefCell<HashMap<String, SourceIdentifier<'static>>>,
	available_columns: RefCell<HashMap<(String, String), ColumnIdentifier<'static>>>,
	transaction: &'t mut T,
}

impl<'t, T: CatalogQueryTransaction> IdentifierResolver<'t, T> {
	pub fn new(transaction: &'t mut T, default_namespace: &'static str) -> Self {
		Self {
			default_namespace,
			source_aliases: RefCell::new(HashMap::new()),
			available_columns: RefCell::new(HashMap::new()),
			transaction,
		}
	}

	/// Get the default namespace
	pub fn default_namespace(&self) -> &'static str {
		self.default_namespace
	}

	/// Register an alias for a source
	pub fn register_alias(&self, alias: String, source: SourceIdentifier<'static>) {
		self.source_aliases.borrow_mut().insert(alias, source);
	}

	/// Clear all aliases (useful when starting a new query scope)
	pub fn clear_aliases(&self) {
		self.source_aliases.borrow_mut().clear();
		self.available_columns.borrow_mut().clear();
	}

	/// Resolve a namespace identifier
	pub fn resolve_schema(&mut self, namespace: &NamespaceIdentifier<'_>) -> Result<NamespaceIdentifier<'static>> {
		let namespace_name = namespace.name.text();

		// Validate namespace exists
		self.transaction.get_namespace_by_name(namespace_name)?;

		Ok(NamespaceIdentifier {
			name: Fragment::Owned(namespace.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified namespace to fully qualified
	pub fn resolve_maybe_schema<'a>(
		&mut self,
		namespace: &MaybeQualifiedNamespaceIdentifier<'a>,
	) -> Result<NamespaceIdentifier<'static>> {
		let namespace_id = NamespaceIdentifier {
			name: namespace.name.clone(),
		};
		self.resolve_schema(&namespace_id)
	}

	/// Resolve a source identifier to fully qualified form
	pub fn resolve_source(&mut self, source: &SourceIdentifier<'_>) -> Result<SourceIdentifier<'static>> {
		// Validate the namespace exists (namespace is always present in
		// fully qualified identifiers)
		let namespace_name = source.namespace().text();

		let _schema = self.transaction.get_namespace_by_name(namespace_name)?;

		let resolved_schema = Fragment::Owned(source.namespace().clone().into_owned());
		let resolved_name = Fragment::Owned(source.name().clone().into_owned());
		let resolved_alias = source.alias().map(|a| Fragment::Owned(a.clone().into_owned()));

		// Create the resolved variant based on the input variant
		let result = match source {
			SourceIdentifier::Table(_) => {
				let mut t = TableIdentifier::new(resolved_schema, resolved_name);
				if let Some(a) = resolved_alias {
					t = t.with_alias(a);
				}
				SourceIdentifier::Table(t)
			}
			SourceIdentifier::TableVirtual(_) => {
				let mut t = TableVirtualIdentifier::new(resolved_schema, resolved_name);
				if let Some(a) = resolved_alias {
					t = t.with_alias(a);
				}
				SourceIdentifier::TableVirtual(t)
			}
			SourceIdentifier::DeferredView(_) => {
				let mut v = DeferredViewIdentifier::new(resolved_schema, resolved_name);
				if let Some(a) = resolved_alias {
					v = v.with_alias(a);
				}
				SourceIdentifier::DeferredView(v)
			}
			SourceIdentifier::TransactionalView(_) => {
				let mut v = TransactionalViewIdentifier::new(resolved_schema, resolved_name);
				if let Some(a) = resolved_alias {
					v = v.with_alias(a);
				}
				SourceIdentifier::TransactionalView(v)
			}
			SourceIdentifier::RingBuffer(_) => {
				let mut r = RingBufferIdentifier::new(resolved_schema, resolved_name);
				if let Some(a) = resolved_alias {
					r = r.with_alias(a);
				}
				SourceIdentifier::RingBuffer(r)
			}
		};

		Ok(result)
	}

	// 	/// Convert and resolve a maybe-qualified source to fully qualified
	// 	pub fn resolve_maybe_source<'a>(
	// 		&mut self,
	// 		source: &MaybeQualifiedSourceIdentifier<'a>,
	// 	) -> Result<SourceIdentifier<'static>> {
	// 		self.resolve_maybe_source_with_validation(source, true)
	// 	}
	//
	// 	/// Convert and resolve a maybe-qualified source to fully qualified
	// with 	/// optional validation
	// 	pub fn resolve_maybe_source_with_validation<'a>(
	// 		&mut self,
	// 		source: &MaybeQualifiedSourceIdentifier<'a>,
	// 		validate_existence: bool,
	// 	) -> Result<SourceIdentifier<'static>> {
	// 		// Determine namespace to use
	// 		let resolved_schema = match &source.namespace {
	// 			Some(namespace) => {
	// 				// User provided explicit namespace - validate
	// 				// it exists
	// 				self.transaction
	// 					.get_namespace_by_name(namespace)?;
	// 				Fragment::Owned(namespace.clone().into_owned())
	// 			}
	// 			None => {
	// 				// No namespace provided - use default namespace
	// 				// Use Internal fragment type to indicate this
	// 				// was injected
	// 				Fragment::Owned(OwnedFragment::Internal {
	// 					text: self
	// 						.default_namespace
	// 						.to_string(),
	// 				})
	// 			}
	// 		};
	//
	// 		// If validation is disabled and kind is Unknown, we can't create a
	// SourceIdentifier 		// This case typically happens during CREATE
	// statements where the source doesn't exist yet
	// 		if !validate_existence && source.kind == SourceKind::Unknown {
	// 			// For CREATE statements, the caller should use the specific
	// resolve methods 			// (resolve_table, resolve_deferred_view, etc.)
	// which handle this case
	// 			return Err(crate::error::IdentifierError::SourceNotFound(
	// 				crate::error::SourceNotFoundError {
	// 					namespace: resolved_schema.text().to_string(),
	// 					name: source.name.text().to_string(),
	// 					fragment: source.name.clone().into_owned(),
	// 				},
	// 			).into());
	// 		}
	//
	// 		// Create the appropriate identifier based on the source kind
	// 		let alias = source.alias.as_ref().map(|a|
	// Fragment::Owned(a.clone().into_owned())); 		let full_source = if
	// validate_existence { 			// Determine the actual type from the
	// catalog 			let source_kind = self.determine_source_kind(
	// 				Some(resolved_schema.text()),
	// 				source.name.clone(),
	// 			)?;
	// 			self.create_source_identifier(
	// 				resolved_schema.clone(),
	// 				Fragment::Owned(source.name.clone().into_owned()),
	// 				source_kind,
	// 				alias,
	// 			)?
	// 		} else {
	// 			// Use the provided kind (should not be Unknown at this point)
	// 			self.create_source_identifier(
	// 				resolved_schema.clone(),
	// 				Fragment::Owned(source.name.clone().into_owned()),
	// 				source.kind,
	// 				alias,
	// 			)?
	// 		};
	//
	// 		Ok(full_source)
	// 	}
	//
	/// Resolve a column identifier to fully qualified form
	pub fn resolve_column(&mut self, column: &ColumnIdentifier<'_>) -> Result<ColumnIdentifier<'static>> {
		let resolved_source = match &column.source {
			ColumnSource::Source {
				namespace,
				source,
			} => {
				// Column is already fully qualified - just
				// validate it exists
				let namespace_name = namespace.text();

				// Validate source exists
				self.validate_source_exists(Some(namespace_name), source.text())?;

				ColumnSource::Source {
					namespace: Fragment::Owned(namespace.clone().into_owned()),
					source: Fragment::Owned(source.clone().into_owned()),
				}
			}
			ColumnSource::Alias(alias) => {
				// Column qualified by alias - check it exists
				if !self.source_aliases.borrow().contains_key(alias.text()) {
					return Err(IdentifierError::UnknownAlias(UnknownAliasError {
						alias: alias.text().to_string(),
					})
					.into());
				}
				ColumnSource::Alias(Fragment::Owned(alias.clone().into_owned()))
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
				namespace,
				source,
			} => {
				// Column qualified by source name
				let resolved_schema = match namespace {
					Some(s) => {
						// Validate namespace exists
						let namespace_name = s.text();
						self.transaction.get_namespace_by_name(namespace_name)?;
						Fragment::Owned(s.clone().into_owned())
					}
					None => {
						// Inject default namespace
						Fragment::Owned(OwnedFragment::Internal {
							text: self.default_namespace.to_string(),
						})
					}
				};

				// Validate source exists
				self.validate_source_exists(Some(resolved_schema.text()), source.text())?;

				ColumnSource::Source {
					namespace: resolved_schema,
					source: Fragment::Owned(source.clone().into_owned()),
				}
			}
			MaybeQualifiedColumnSource::Alias(alias) => {
				// Column qualified by alias - check it
				// exists
				if !self.source_aliases.borrow().contains_key(alias.text()) {
					return Err(IdentifierError::UnknownAlias(UnknownAliasError {
						alias: alias.text().to_string(),
					})
					.into());
				}
				ColumnSource::Alias(Fragment::Owned(alias.clone().into_owned()))
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
				let matching_sources = self.find_column_sources(column_name);

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
						}
						.into());
					}
					1 => {
						// Unambiguous - qualify
						// with the single
						// source
						let (_, source_id) = matching_sources.into_iter().next().unwrap();
						ColumnSource::Source {
							namespace: source_id.namespace().clone(),
							source: source_id.name().clone(),
						}
					}
					_ => {
						// Ambiguous - report
						// error
						let sources: Vec<String> =
							matching_sources.iter().map(|(name, _)| name.clone()).collect();
						return Err(IdentifierError::AmbiguousColumn(
							crate::error::AmbiguousColumnError {
								column: column_name.to_string(),
								sources,
							},
						)
						.into());
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
	pub fn resolve_function(&self, func: &FunctionIdentifier<'_>) -> Result<FunctionIdentifier<'static>> {
		// Validate function exists in catalog
		let namespaces: Vec<String> = func.namespaces.iter().map(|f| f.text().to_string()).collect();
		let function_name = func.name.text();

		// TODO: Validate function exists once CatalogQueryTransaction
		// supports it
		if false {
			return Err(IdentifierError::FunctionNotFound(FunctionNotFoundError {
				namespaces,
				name: function_name.to_string(),
			})
			.into());
		}

		Ok(FunctionIdentifier {
			namespaces: func.namespaces.iter().map(|f| Fragment::Owned(f.clone().into_owned())).collect(),
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
	pub fn resolve_sequence(&self, seq: &SequenceIdentifier<'_>) -> Result<SequenceIdentifier<'static>> {
		// Validate sequence exists
		let namespace_name = seq.namespace.text();
		let seq_name = seq.name.text();

		// TODO: Add sequence validation once CatalogQueryTransaction
		// supports it
		if false {
			return Err(IdentifierError::SequenceNotFound {
				namespace: namespace_name.to_string(),
				name: seq_name.to_string(),
			}
			.into());
		}

		Ok(SequenceIdentifier {
			namespace: Fragment::Owned(seq.namespace.clone().into_owned()),
			name: Fragment::Owned(seq.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified sequence to fully qualified
	pub fn resolve_maybe_sequence<'a>(
		&mut self,
		seq: &MaybeQualifiedSequenceIdentifier<'a>,
	) -> Result<SequenceIdentifier<'static>> {
		// Determine namespace to use
		let resolved_schema = match &seq.namespace {
			Some(namespace) => {
				// Validate namespace exists
				self.transaction.get_namespace_by_name(namespace)?;
				Fragment::Owned(namespace.clone().into_owned())
			}
			None => {
				// Inject default namespace
				let default_namespace = self.default_namespace;
				Fragment::Owned(OwnedFragment::Internal {
					text: default_namespace.to_string(),
				})
			}
		};

		// Create fully qualified sequence identifier
		let full_seq = SequenceIdentifier {
			namespace: resolved_schema,
			name: Fragment::Owned(seq.name.clone().into_owned()),
		};
		self.resolve_sequence(&full_seq)
	}

	/// Resolve an index identifier
	pub fn resolve_index(&self, idx: &IndexIdentifier<'_>) -> Result<IndexIdentifier<'static>> {
		// Validate index exists
		let namespace_name = idx.namespace.text();
		let table_name = idx.table.text();
		let index_name = idx.name.text();

		// TODO: Add index validation once CatalogQueryTransaction
		// supports it
		if false {
			return Err(IdentifierError::IndexNotFound {
				namespace: namespace_name.to_string(),
				table: table_name.to_string(),
				name: index_name.to_string(),
			}
			.into());
		}

		Ok(IndexIdentifier {
			namespace: Fragment::Owned(idx.namespace.clone().into_owned()),
			table: Fragment::Owned(idx.table.clone().into_owned()),
			name: Fragment::Owned(idx.name.clone().into_owned()),
		})
	}

	/// Convert and resolve a maybe-qualified index to fully qualified
	pub fn resolve_maybe_index<'a>(
		&mut self,
		idx: &MaybeQualifiedIndexIdentifier<'a>,
	) -> Result<IndexIdentifier<'static>> {
		// Determine namespace to use
		let resolved_schema = match &idx.namespace {
			Some(namespace) => {
				self.transaction.get_namespace_by_name(namespace)?;
				Fragment::Owned(namespace.clone().into_owned())
			}
			None => {
				// Inject default namespace
				let default_namespace = self.default_namespace;
				Fragment::Owned(OwnedFragment::Internal {
					text: default_namespace.to_string(),
				})
			}
		};

		// Create fully qualified index identifier
		let full_idx = IndexIdentifier {
			namespace: resolved_schema,
			table: Fragment::Owned(idx.table.clone().into_owned()),
			name: Fragment::Owned(idx.name.clone().into_owned()),
		};
		self.resolve_index(&full_idx)
	}

	// Helper methods

	/// Create a SourceIdentifier from resolved components and source kind
	// Helper method to create a source identifier from an unresolved source
	// by determining its type from the catalog
	fn create_source_identifier_from_catalog(
		&mut self,
		namespace: Fragment<'static>,
		name: Fragment<'static>,
		alias: Option<Fragment<'static>>,
	) -> Result<SourceIdentifier<'static>> {
		let namespace_str = namespace.text();
		let name_str = name.text();

		// Check if it's a system table (virtual table in system
		// namespace)
		if namespace_str == "system" {
			// Check if it's a known system table
			if Self::is_system_table(name_str) {
				let mut t = TableVirtualIdentifier::new(namespace, name);
				if let Some(a) = alias {
					t = t.with_alias(a);
				}
				return Ok(SourceIdentifier::TableVirtual(t));
			}
		}

		// Get the namespace ID
		let ns = self.transaction.get_namespace_by_name(namespace_str)?;

		// Check for regular table
		if self.transaction.find_table_by_name(ns.id, name_str)?.is_some() {
			let mut t = TableIdentifier::new(namespace, name);
			if let Some(a) = alias {
				t = t.with_alias(a);
			}
			return Ok(SourceIdentifier::Table(t));
		}

		// Check for view and determine its type
		if let Some(view) = self.transaction.find_view_by_name(ns.id, name_str)? {
			match view.kind {
				ViewKind::Deferred => {
					let mut v = DeferredViewIdentifier::new(namespace, name);
					if let Some(a) = alias {
						v = v.with_alias(a);
					}
					Ok(SourceIdentifier::DeferredView(v))
				}
				ViewKind::Transactional => {
					let mut v = TransactionalViewIdentifier::new(namespace, name);
					if let Some(a) = alias {
						v = v.with_alias(a);
					}
					Ok(SourceIdentifier::TransactionalView(v))
				}
			}
		} else {
			// Source not found
			Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: namespace_str.to_string(),
				name: name_str.to_string(),
				fragment: name.clone().into_owned(),
			})
			.into())
		}
	}

	// Helper to check if a source exists in the catalog
	fn check_source_exists<'a>(&mut self, namespace: Option<&str>, name: impl IntoFragment<'a>) -> Result<bool> {
		let default_namespace = self.default_namespace;
		let namespace_str = namespace.unwrap_or_else(|| &*default_namespace);
		let name = name.into_fragment();

		// Check if it's a system table
		if namespace_str == "system" && Self::is_system_table(name.text()) {
			return Ok(true);
		}

		// Get the namespace ID
		let ns = self.transaction.get_namespace_by_name(namespace_str)?;

		// Check for table or view
		if self.transaction.find_table_by_name(ns.id, name.text())?.is_some() {
			return Ok(true);
		}

		if self.transaction.find_view_by_name(ns.id, name.text())?.is_some() {
			return Ok(true);
		}

		Ok(false)
	}

	#[allow(dead_code)]
	fn resolve_schema_fragment(&mut self, namespace: &Option<Fragment<'_>>) -> Result<Option<Fragment<'static>>> {
		Ok(match namespace {
			Some(s) => {
				self.transaction.get_namespace_by_name(s)?;
				Some(Fragment::Owned(s.clone().into_owned()))
			}
			None => {
				// Inject default namespace if available
				let default_namespace = self.default_namespace;
				Some(Fragment::Owned(OwnedFragment::Internal {
					text: default_namespace.to_string(),
				}))
			}
		})
	}

	fn validate_source_exists(&mut self, namespace: Option<&str>, name: &str) -> Result<()> {
		// Validate source exists
		if !self.check_source_exists(namespace, name)? {
			let namespace_str = namespace.unwrap_or(self.default_namespace);
			return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: namespace_str.to_string(),
				name: name.to_string(),
				fragment: OwnedFragment::None,
			})
			.into());
		}
		Ok(())
	}

	#[allow(dead_code)]
	fn find_column_sources(&self, column_name: &str) -> Vec<(String, SourceIdentifier<'static>)> {
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
	fn source_has_column(&self, source: &SourceIdentifier<'static>, _column_name: &str) -> bool {
		let _schema = source.namespace().text();
		let _source_name = source.name().text();

		match source {
			SourceIdentifier::Table(_)
			| SourceIdentifier::TableVirtual(_)
			| SourceIdentifier::RingBuffer(_) => {
				// TODO: Check table/ring buffer has column once
				// CatalogQueryTransaction supports it
				true
			}
			SourceIdentifier::DeferredView(_) | SourceIdentifier::TransactionalView(_) => {
				// TODO: Check view has column once
				// CatalogQueryTransaction supports it
				true
			}
		}
	}

	/// Register available columns for a source
	/// This should be called after resolving a source in FROM/JOIN
	pub fn register_source_columns(&mut self, source: &SourceIdentifier<'static>) -> Result<()> {
		let _schema = source.namespace().text();
		let _source_name = source.name().text();
		let effective_name = source.effective_name();

		// Get columns based on source type
		use reifydb_core::interface::ColumnDef;
		let columns: Vec<ColumnDef> = match source {
			SourceIdentifier::Table(_)
			| SourceIdentifier::TableVirtual(_)
			| SourceIdentifier::RingBuffer(_) => {
				// TODO: Get table/ring buffer columns once
				// CatalogQueryTransaction supports it
				Vec::new()
			}
			SourceIdentifier::DeferredView(_) | SourceIdentifier::TransactionalView(_) => {
				// TODO: Get view columns once
				// CatalogQueryTransaction supports it
				Vec::new()
			}
		};

		// Register each column as available
		for column in columns {
			let col_ident = ColumnIdentifier {
				source: if source.alias().is_some() {
					ColumnSource::Alias(source.alias().unwrap().clone())
				} else {
					ColumnSource::Source {
						namespace: source.namespace().clone(),
						source: source.name().clone(),
					}
				},
				name: Fragment::Owned(OwnedFragment::Internal {
					text: column.name.clone(),
				}),
			};

			self.available_columns
				.borrow_mut()
				.insert((effective_name.to_string(), column.name), col_ident);
		}

		Ok(())
	}
}

// New resolution methods that return resolved types
impl<'t, T: CatalogQueryTransaction> IdentifierResolver<'t, T> {
	/// Build a resolved namespace
	pub fn build_resolved_namespace<'a>(
		&mut self,
		ident: NamespaceIdentifier<'a>,
	) -> Result<Rc<ResolvedNamespace<'a>>> {
		let namespace_name = ident.name.text();

		// Lookup in catalog - get_namespace_by_name returns
		// Result<NamespaceDef>
		let def = self.transaction.get_namespace_by_name(namespace_name)?;

		let resolved = Rc::new(ResolvedNamespace::new(ident, def));

		Ok(resolved)
	}

	/// Build a resolved table
	pub fn build_resolved_table<'a>(&mut self, ident: SourceIdentifier<'a>) -> Result<ResolvedTable<'a>> {
		// Extract the TableIdentifier from the enum
		let table_ident = match ident {
			SourceIdentifier::Table(t) => t,
			_ => {
				// Create a TableIdentifier from other variants
				TableIdentifier {
					namespace: ident.namespace().clone(),
					name: ident.name().clone(),
					alias: ident.alias().cloned(),
				}
			}
		};

		// Resolve namespace first
		let namespace_ident = NamespaceIdentifier {
			name: table_ident.namespace.clone(),
		};
		let namespace = self.build_resolved_namespace(namespace_ident)?;

		// Lookup table in catalog
		let table_name = table_ident.name.text();
		let def = self.transaction.find_table_by_name(namespace.def.id, table_name)?.ok_or_else(
			|| -> reifydb_core::Error {
				// Return an error instead of panicking
				crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
					namespace: namespace.def.name.clone(),
					name: table_name.to_string(),
					fragment: table_ident.name.clone().into_owned(),
				})
				.into()
			},
		)?;

		Ok(ResolvedTable::new(table_ident, namespace, def))
	}

	/// Build a resolved view
	pub fn build_resolved_view<'a>(&mut self, ident: SourceIdentifier<'a>) -> Result<ResolvedView<'a>> {
		// Resolve namespace first
		let namespace_ident = NamespaceIdentifier {
			name: ident.namespace().clone(),
		};
		let namespace = self.build_resolved_namespace(namespace_ident)?;

		// Lookup view in catalog
		let view_name = ident.name().text();
		let def = self.transaction.find_view_by_name(namespace.def.id, view_name)?.ok_or_else(
			|| -> reifydb_core::Error {
				// Return an error instead of panicking
				crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
					namespace: namespace.def.name.clone(),
					name: view_name.to_string(),
					fragment: ident.name().clone().into_owned(),
				})
				.into()
			},
		)?;

		Ok(ResolvedView::new(ident, namespace, def))
	}

	/// Build a resolved source from an unresolved identifier
	pub fn build_resolved_source_from_unresolved<'a>(
		&mut self,
		ident: UnresolvedSourceIdentifier<'a>,
	) -> Result<Rc<ResolvedSource<'a>>> {
		// Try to determine the source type from the catalog
		let name_text = ident.name.text();

		// Create the resolved namespace fragment and get namespace text
		let (namespace_fragment, namespace_text) = match ident.namespace {
			Some(ns) => {
				let text = ns.text().to_string();
				(Fragment::Owned(ns.into_owned()), text)
			}
			None => {
				let text = self.default_namespace.to_string();
				(
					Fragment::Owned(OwnedFragment::Internal {
						text: text.clone(),
					}),
					text,
				)
			}
		};

		// First check if it's a system table
		if Self::is_system_table(name_text) {
			let mut t = TableVirtualIdentifier::new(
				namespace_fragment.clone(),
				Fragment::Owned(ident.name.into_owned()),
			);
			if let Some(alias) = ident.alias {
				t = t.with_alias(alias);
			}
			let source = SourceIdentifier::TableVirtual(t);
			return self.build_resolved_source(source);
		}

		// Try to find it as a table
		let ns = self.transaction.get_namespace_by_name(&namespace_text)?;
		if self.transaction.find_table_by_name(ns.id, name_text)?.is_some() {
			let mut t = TableIdentifier::new(
				namespace_fragment.clone(),
				Fragment::Owned(ident.name.into_owned()),
			);
			if let Some(alias) = ident.alias {
				t = t.with_alias(alias);
			}
			let source = SourceIdentifier::Table(t);
			return self.build_resolved_source(source);
		}

		// Try to find it as a view
		if let Some(view) = self.transaction.find_view_by_name(ns.id, name_text)? {
			use reifydb_core::interface::ViewKind;
			match view.kind {
				ViewKind::Deferred => {
					let mut v = DeferredViewIdentifier::new(
						namespace_fragment.clone(),
						Fragment::Owned(ident.name.into_owned()),
					);
					if let Some(alias) = ident.alias {
						v = v.with_alias(alias);
					}
					let source = SourceIdentifier::DeferredView(v);
					self.build_resolved_source(source)
				}
				ViewKind::Transactional => {
					let mut v = TransactionalViewIdentifier::new(
						namespace_fragment,
						Fragment::Owned(ident.name.into_owned()),
					);
					if let Some(alias) = ident.alias {
						v = v.with_alias(alias);
					}
					let source = SourceIdentifier::TransactionalView(v);
					self.build_resolved_source(source)
				}
			}
		} else {
			// Source not found
			Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: namespace_text.to_string(),
				name: name_text.to_string(),
				fragment: ident.name.into_owned(),
			})
			.into())
		}
	}

	/// Build a resolved source (any type)
	pub fn build_resolved_source<'a>(&mut self, ident: SourceIdentifier<'a>) -> Result<Rc<ResolvedSource<'a>>> {
		let namespace_name = ident.namespace().text();
		let source_name = ident.name().text();

		// Check if it's a system virtual table
		if namespace_name == "system" {
			if let Some(def) = Self::get_system_table_def(source_name) {
				// For system tables, we need to get the system
				// namespace
				let namespace_ident = NamespaceIdentifier {
					name: ident.namespace().clone(),
				};
				// Build a resolved namespace for "system"
				// Since system namespace might not exist in the
				// catalog, we create a synthetic one
				let namespace = Rc::new(ResolvedNamespace::new(
					namespace_ident,
					reifydb_core::interface::NamespaceDef {
						id: reifydb_core::interface::NamespaceId(1), // System namespace ID
						name: "system".to_string(),
					},
				));

				// Extract or create TableVirtualIdentifier
				let virtual_ident = match ident {
					SourceIdentifier::TableVirtual(t) => t,
					_ => TableVirtualIdentifier {
						namespace: ident.namespace().clone(),
						name: ident.name().clone(),
						alias: ident.alias().cloned(),
					},
				};
				let virtual_table = ResolvedTableVirtual::new(
					virtual_ident,
					namespace,
					Arc::try_unwrap(def).unwrap_or_else(|arc| (*arc).clone()),
				);
				let resolved = Rc::new(ResolvedSource::TableVirtual(virtual_table));
				return Ok(resolved);
			}
		}

		// Try to resolve as table first
		if let Ok(table) = self.build_resolved_table(ident.clone()) {
			let resolved = Rc::new(ResolvedSource::Table(table));
			return Ok(resolved);
		}

		// Try to resolve as view
		if let Ok(view) = self.build_resolved_view(ident.clone()) {
			// Check view kind and create appropriate resolved type
			let resolved = match view.def.kind {
				ViewKind::Deferred => {
					// Extract or create
					// DeferredViewIdentifier
					let deferred_ident = match ident {
						SourceIdentifier::DeferredView(d) => d,
						_ => DeferredViewIdentifier {
							namespace: ident.namespace().clone(),
							name: ident.name().clone(),
							alias: ident.alias().cloned(),
						},
					};
					let deferred =
						ResolvedDeferredView::new(deferred_ident, view.namespace, view.def);
					Rc::new(ResolvedSource::DeferredView(deferred))
				}
				ViewKind::Transactional => {
					// Extract or create
					// TransactionalViewIdentifier
					let trans_ident = match ident {
						SourceIdentifier::TransactionalView(t) => t,
						_ => TransactionalViewIdentifier {
							namespace: ident.namespace().clone(),
							name: ident.name().clone(),
							alias: ident.alias().cloned(),
						},
					};
					let transactional =
						ResolvedTransactionalView::new(trans_ident, view.namespace, view.def);
					Rc::new(ResolvedSource::TransactionalView(transactional))
				}
			};

			return Ok(resolved);
		}

		// Source not found - return proper error
		Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
			namespace: namespace_name.to_string(),
			name: source_name.to_string(),
			fragment: ident.name().clone().into_owned(),
		})
		.into())
	}

	/// Helper to check if a name is a known system table
	fn is_system_table(name: &str) -> bool {
		matches!(
			name,
			"sequences"
				| "namespaces" | "tables" | "views"
				| "columns" | "column_policies"
				| "primary_keys" | "primary_key_columns"
				| "versions"
		)
	}

	/// Helper to get system table definition
	fn get_system_table_def(name: &str) -> Option<Arc<TableVirtualDef>> {
		match name {
			"sequences" => Some(SystemCatalog::get_system_sequences_table_def()),
			"namespaces" => Some(SystemCatalog::get_system_namespaces_table_def()),
			"tables" => Some(SystemCatalog::get_system_tables_table_def()),
			"views" => Some(SystemCatalog::get_system_views_table_def()),
			"columns" => Some(SystemCatalog::get_system_columns_table_def()),
			"column_policies" => Some(SystemCatalog::get_system_column_policies_table_def()),
			"primary_keys" => Some(SystemCatalog::get_system_primary_keys_table_def()),
			"primary_key_columns" => Some(SystemCatalog::get_system_primary_key_columns_table_def()),
			"versions" => Some(SystemCatalog::get_system_versions_table_def()),
			_ => None,
		}
	}

	/// Build a resolved column
	pub fn build_resolved_column<'a>(&mut self, ident: ColumnIdentifier<'a>) -> Result<ResolvedColumn<'a>> {
		// First resolve the source
		let source_ident = match &ident.source {
			ColumnSource::Source {
				namespace,
				source,
			} => {
				// We don't know the source type yet, so
				// we need to determine it from catalog
				self.create_source_identifier_from_catalog(
					Fragment::Owned(namespace.clone().into_owned()),
					Fragment::Owned(source.clone().into_owned()),
					None, // Columns don't use aliases on their source references
				)?
			}
			ColumnSource::Alias(alias) => {
				// Lookup alias in current query context
				self.source_aliases.borrow().get(alias.text()).cloned().ok_or_else(
					|| -> reifydb_core::Error {
						crate::error::IdentifierError::UnknownAlias(
							crate::error::UnknownAliasError {
								alias: alias.text().to_string(),
							},
						)
						.into()
					},
				)?
			}
		};

		let source = self.build_resolved_source(source_ident)?;

		// Find column in source
		let column_name = ident.name.text();
		let def = source
			.find_column(column_name)
			.ok_or_else(|| -> reifydb_core::Error {
				crate::error::IdentifierError::AmbiguousColumn(crate::error::AmbiguousColumnError {
					column: column_name.to_string(),
					sources: vec![source.effective_name().to_string()],
				})
				.into()
			})?
			.clone();

		Ok(ResolvedColumn::new(ident, source, def))
	}

	/// Resolve a MaybeQualifiedTableIdentifier specifically
	pub fn resolve_maybe_qualified_table<'a>(
		&mut self,
		source: &MaybeQualifiedTableIdentifier<'a>,
		validate_existence: bool,
	) -> Result<TableIdentifier<'static>> {
		// Get the table name
		let name_text = source.name.text();

		// Always validate namespace exists (can't create table in
		// non-existent namespace) Get namespace, passing the fragment
		// if available for error reporting
		let ns = if let Some(namespace_fragment) = &source.namespace {
			self.transaction.get_namespace_by_name(namespace_fragment.clone())?
		} else {
			self.transaction.get_namespace_by_name(self.default_namespace)?
		};

		// Only validate table existence if requested
		if validate_existence {
			if self.transaction.find_table_by_name(ns.id, name_text)?.is_none() {
				return Err(crate::error::IdentifierError::SourceNotFound(
					crate::error::SourceNotFoundError {
						namespace: ns.name.clone(),
						name: name_text.to_string(),
						fragment: source.name.clone().into_owned(),
					},
				)
				.into());
			}
		}

		// Get namespace text for creating the identifier
		let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(self.default_namespace);

		// Create the TableIdentifier preserving original fragments
		use reifydb_type::{Fragment, OwnedFragment};

		// For namespace, use the original fragment if available,
		// otherwise create Internal
		let namespace_fragment = if let Some(ns_frag) = &source.namespace {
			Fragment::Owned(ns_frag.clone().into_owned())
		} else {
			Fragment::Owned(OwnedFragment::Internal {
				text: namespace_text.to_string(),
			})
		};

		// For name, always preserve the original fragment for error
		// reporting
		let name_fragment = Fragment::Owned(source.name.clone().into_owned());

		let mut table = TableIdentifier::new(namespace_fragment, name_fragment);
		if let Some(alias) = &source.alias {
			table.alias = Some(Fragment::Owned(alias.clone().into_owned()));
		}

		Ok(table)
	}

	/// Resolve a MaybeQualifiedRingBufferIdentifier specifically
	pub fn resolve_maybe_qualified_ring_buffer<'a>(
		&mut self,
		source: &MaybeQualifiedRingBufferIdentifier<'a>,
		validate_existence: bool,
	) -> Result<RingBufferIdentifier<'static>> {
		// Get the ring buffer name
		let name_text = source.name.text();

		// Always validate namespace exists (can't create ring buffer in
		// non-existent namespace) Get namespace, passing the fragment
		// if available for error reporting
		let ns = if let Some(namespace_fragment) = &source.namespace {
			self.transaction.get_namespace_by_name(namespace_fragment.clone())?
		} else {
			self.transaction.get_namespace_by_name(self.default_namespace)?
		};

		// Only validate ring buffer existence if requested
		if validate_existence {
			if self.transaction.find_ring_buffer_by_name(ns.id, name_text)?.is_none() {
				return Err(crate::error::IdentifierError::SourceNotFound(
					crate::error::SourceNotFoundError {
						namespace: ns.name.clone(),
						name: name_text.to_string(),
						fragment: source.name.clone().into_owned(),
					},
				)
				.into());
			}
		}

		// Create namespace text for the identifier
		let namespace_text = ns.name.as_str();

		use reifydb_type::{Fragment, OwnedFragment};

		// For namespace, use the original fragment if available,
		// otherwise create Internal
		let namespace_fragment = if let Some(ns_frag) = &source.namespace {
			Fragment::Owned(ns_frag.clone().into_owned())
		} else {
			Fragment::Owned(OwnedFragment::Internal {
				text: namespace_text.to_string(),
			})
		};

		// For name, always preserve the original fragment for error
		// reporting
		let name_fragment = Fragment::Owned(source.name.clone().into_owned());

		let mut ring_buffer = RingBufferIdentifier::new(namespace_fragment, name_fragment);

		// Handle alias if present
		if let Some(alias) = &source.alias {
			ring_buffer = ring_buffer.with_alias(Fragment::Owned(alias.clone().into_owned()));
		}

		Ok(ring_buffer)
	}

	/// Resolve a MaybeQualifiedDeferredViewIdentifier specifically
	pub fn resolve_maybe_qualified_deferred_view<'a>(
		&mut self,
		source: &MaybeQualifiedDeferredViewIdentifier<'a>,
		validate_existence: bool,
	) -> Result<DeferredViewIdentifier<'static>> {
		// Get the view name
		let name_text = source.name.text();

		// Always validate namespace exists (can't create view in
		// non-existent namespace) Get namespace, passing the fragment
		// if available for error reporting
		let ns = if let Some(namespace_fragment) = &source.namespace {
			self.transaction.get_namespace_by_name(namespace_fragment.clone())?
		} else {
			self.transaction.get_namespace_by_name(self.default_namespace)?
		};

		// Only validate view existence if requested
		if validate_existence {
			if self.transaction.find_view_by_name(ns.id, name_text)?.is_none() {
				return Err(crate::error::IdentifierError::SourceNotFound(
					crate::error::SourceNotFoundError {
						namespace: ns.name.clone(),
						name: name_text.to_string(),
						fragment: source.name.clone().into_owned(),
					},
				)
				.into());
			}
		}

		// Get namespace text for creating the identifier
		let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(self.default_namespace);

		// Create the DeferredViewIdentifier preserving original
		// fragments
		use reifydb_type::{Fragment, OwnedFragment};

		// For namespace, use the original fragment if available,
		// otherwise create Internal
		let namespace_fragment = if let Some(ns_frag) = &source.namespace {
			Fragment::Owned(ns_frag.clone().into_owned())
		} else {
			Fragment::Owned(OwnedFragment::Internal {
				text: namespace_text.to_string(),
			})
		};

		// For name, always preserve the original fragment for error
		// reporting
		let name_fragment = Fragment::Owned(source.name.clone().into_owned());

		let mut view = DeferredViewIdentifier::new(namespace_fragment, name_fragment);
		if let Some(alias) = &source.alias {
			view.alias = Some(Fragment::Owned(alias.clone().into_owned()));
		}

		Ok(view)
	}

	/// Resolve a MaybeQualifiedTransactionalViewIdentifier specifically
	pub fn resolve_maybe_qualified_transactional_view<'a>(
		&mut self,
		source: &MaybeQualifiedTransactionalViewIdentifier<'a>,
		validate_existence: bool,
	) -> Result<TransactionalViewIdentifier<'static>> {
		// Get the view name
		let name_text = source.name.text();

		// Always validate namespace exists (can't create view in
		// non-existent namespace) Get namespace, passing the fragment
		// if available for error reporting
		let ns = if let Some(namespace_fragment) = &source.namespace {
			self.transaction.get_namespace_by_name(namespace_fragment.clone())?
		} else {
			self.transaction.get_namespace_by_name(self.default_namespace)?
		};

		// Only validate view existence if requested
		if validate_existence {
			if self.transaction.find_view_by_name(ns.id, name_text)?.is_none() {
				return Err(crate::error::IdentifierError::SourceNotFound(
					crate::error::SourceNotFoundError {
						namespace: ns.name.clone(),
						name: name_text.to_string(),
						fragment: source.name.clone().into_owned(),
					},
				)
				.into());
			}
		}

		// Get namespace text for creating the identifier
		let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(self.default_namespace);

		// Create the TransactionalViewIdentifier preserving original
		// fragments
		use reifydb_type::{Fragment, OwnedFragment};

		// For namespace, use the original fragment if available,
		// otherwise create Internal
		let namespace_fragment = if let Some(ns_frag) = &source.namespace {
			Fragment::Owned(ns_frag.clone().into_owned())
		} else {
			Fragment::Owned(OwnedFragment::Internal {
				text: namespace_text.to_string(),
			})
		};

		// For name, always preserve the original fragment for error
		// reporting
		let name_fragment = Fragment::Owned(source.name.clone().into_owned());

		let mut view = TransactionalViewIdentifier::new(namespace_fragment, name_fragment);
		if let Some(alias) = &source.alias {
			view.alias = Some(Fragment::Owned(alias.clone().into_owned()));
		}

		Ok(view)
	}

	/// Resolve a MaybeQualifiedViewIdentifier (generic view)
	pub fn resolve_maybe_qualified_view<'a>(
		&mut self,
		source: &MaybeQualifiedViewIdentifier<'a>,
		validate_existence: bool,
	) -> Result<SourceIdentifier<'static>> {
		// Get the view name
		let name_text = source.name.text();

		// Validate if requested
		if validate_existence {
			// Get namespace, passing the fragment if available for
			// error reporting
			let ns = if let Some(namespace_fragment) = &source.namespace {
				self.transaction.get_namespace_by_name(namespace_fragment.clone())?
			} else {
				self.transaction.get_namespace_by_name(self.default_namespace)?
			};

			// Check if it exists as a view and determine its type
			if self.transaction.find_view_by_name(ns.id, name_text)?.is_none() {
				return Err(crate::error::IdentifierError::SourceNotFound(
					crate::error::SourceNotFoundError {
						namespace: ns.name.clone(),
						name: name_text.to_string(),
						fragment: source.name.clone().into_owned(),
					},
				)
				.into());
			}

			// Get the view to determine its type
			let view = self.transaction.find_view_by_name(ns.id, name_text)?.ok_or_else(|| {
				crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
					namespace: ns.name.clone(),
					name: name_text.to_string(),
					fragment: source.name.clone().into_owned(),
				})
			})?;

			// Get namespace text for creating the identifier
			let namespace_text =
				source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(self.default_namespace);

			// Create owned fragments
			use reifydb_type::{Fragment, OwnedFragment};
			let namespace_fragment = Fragment::Owned(OwnedFragment::Internal {
				text: namespace_text.to_string(),
			});
			let name_fragment = Fragment::Owned(OwnedFragment::Internal {
				text: name_text.to_string(),
			});

			// Create the appropriate view identifier based on type
			match view.kind {
				ViewKind::Deferred => {
					let mut v = DeferredViewIdentifier::new(namespace_fragment, name_fragment);
					if let Some(alias) = &source.alias {
						v.alias = Some(Fragment::Owned(alias.clone().into_owned()));
					}
					Ok(SourceIdentifier::DeferredView(v))
				}
				ViewKind::Transactional => {
					let mut v = TransactionalViewIdentifier::new(namespace_fragment, name_fragment);
					if let Some(alias) = &source.alias {
						v.alias = Some(Fragment::Owned(alias.clone().into_owned()));
					}
					Ok(SourceIdentifier::TransactionalView(v))
				}
			}
		} else {
			// Get namespace text for creating the identifier
			let namespace_text =
				source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(self.default_namespace);

			// Create owned fragments
			use reifydb_type::{Fragment, OwnedFragment};
			let namespace_fragment = Fragment::Owned(OwnedFragment::Internal {
				text: namespace_text.to_string(),
			});
			let name_fragment = Fragment::Owned(OwnedFragment::Internal {
				text: name_text.to_string(),
			});

			// For ALTER VIEW without validation, we can't determine
			// the type Default to DeferredView for now
			let mut v = DeferredViewIdentifier::new(namespace_fragment, name_fragment);
			if let Some(alias) = &source.alias {
				v.alias = Some(Fragment::Owned(alias.clone().into_owned()));
			}
			Ok(SourceIdentifier::DeferredView(v))
		}
	}

	/// Resolve an unresolved source identifier (used in FROM clauses where
	/// type is unknown)
	pub fn resolve_unresolved_source<'a>(
		&mut self,
		source: &UnresolvedSourceIdentifier<'a>,
	) -> Result<SourceIdentifier<'static>> {
		// Resolve namespace - use default if not provided
		let namespace_fragment = match &source.namespace {
			Some(ns) => Fragment::Owned(ns.clone().into_owned()),
			None => Fragment::Owned(OwnedFragment::Internal {
				text: self.default_namespace.to_string(),
			}),
		};
		let name_fragment = Fragment::Owned(source.name.clone().into_owned());
		let alias_fragment = source.alias.as_ref().map(|a| Fragment::Owned(a.clone().into_owned()));

		// Use helper to determine type from catalog
		self.create_source_identifier_from_catalog(namespace_fragment, name_fragment, alias_fragment)
	}
}
