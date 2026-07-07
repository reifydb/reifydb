// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::interface::catalog::{
	id::{ColumnId, NamespaceId, TableId},
	relationship::RelationshipCardinality,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	ast::{ast::AstRelationshipCardinality, identifier::MaybeQualifiedTableIdentifier},
	bump::BumpFragment,
	nodes::{CreateRelationshipNode, RelationshipJunction},
	plan::{
		logical::{self, resolver::DEFAULT_NAMESPACE},
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_relationship(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateRelationshipNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let namespace = resolve_relationship_namespace(self, rx, &create.source)?;
		let (source_table, source_column) =
			resolve_table_column(self, rx, namespace, &create.source, &create.source_column)?;
		let (target_table, target_column) =
			resolve_table_column(self, rx, namespace, &create.target, &create.target_column)?;

		let cardinality = map_cardinality(create.cardinality);

		let junction = match (cardinality, create.junction) {
			(RelationshipCardinality::ManyToMany, Some(j)) => {
				let (jtable, jsrc) =
					resolve_table_column(self, rx, namespace, &j.table, &j.source_column)?;
				let (_jtable_again, jtgt) =
					resolve_table_column(self, rx, namespace, &j.table, &j.target_column)?;
				Some(RelationshipJunction {
					table: jtable,
					source_column: jsrc,
					target_column: jtgt,
				})
			}
			(RelationshipCardinality::ManyToMany, None) => {
				return Err(CatalogError::InvalidRelationship {
					reason: "N:M cardinality requires a THROUGH junction table".to_string(),
					fragment: create.name.to_owned(),
				}
				.into());
			}
			(_, Some(_)) => {
				return Err(CatalogError::InvalidRelationship {
					reason: "THROUGH junction is only valid for N:M cardinality".to_string(),
					fragment: create.name.to_owned(),
				}
				.into());
			}
			(_, None) => None,
		};

		Ok(PhysicalPlan::CreateRelationship(CreateRelationshipNode {
			namespace,
			name: self.interner.intern_fragment(&create.name),
			source_table,
			source_column,
			target_table,
			target_column,
			junction,
			cardinality,
		}))
	}
}

fn resolve_relationship_namespace<'bump>(
	compiler: &Compiler<'bump>,
	rx: &mut Transaction<'_>,
	source: &MaybeQualifiedTableIdentifier<'_>,
) -> Result<NamespaceId> {
	let segments: Vec<&str> = source.namespace.iter().map(|n| n.text()).collect();
	if segments.is_empty() {
		let Some(ns) = compiler.catalog.find_namespace_by_name(rx, DEFAULT_NAMESPACE)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: DEFAULT_NAMESPACE.to_string(),
				name: String::new(),
				fragment: Fragment::internal(DEFAULT_NAMESPACE),
			}
			.into());
		};
		return Ok(ns.id());
	}
	let Some(ns) = compiler.catalog.find_namespace_by_segments(rx, &segments)? else {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: segments.join("::"),
			name: String::new(),
			fragment: Fragment::internal(segments.join("::")),
		}
		.into());
	};
	Ok(ns.id())
}

fn resolve_table_column<'bump>(
	compiler: &Compiler<'bump>,
	rx: &mut Transaction<'_>,
	default_namespace: NamespaceId,
	table: &MaybeQualifiedTableIdentifier<'_>,
	column: &BumpFragment<'_>,
) -> Result<(TableId, ColumnId)> {
	let namespace = if table.namespace.is_empty() {
		default_namespace
	} else {
		let segments: Vec<&str> = table.namespace.iter().map(|n| n.text()).collect();
		let Some(ns) = compiler.catalog.find_namespace_by_segments(rx, &segments)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: segments.join("::"),
				name: String::new(),
				fragment: Fragment::internal(segments.join("::")),
			}
			.into());
		};
		ns.id()
	};

	let table_name = table.name.text();
	let Some(t) = compiler.catalog.find_table_by_name(rx, namespace, table_name)? else {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Table,
			namespace: namespace.0.to_string(),
			name: table_name.to_string(),
			fragment: table.name.to_owned(),
		}
		.into());
	};

	let columns = compiler.catalog.list_columns(rx, t.id)?;
	let column_name = column.text();
	let Some(c) = columns.iter().find(|c| c.name == column_name) else {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Column,
			namespace: namespace.0.to_string(),
			name: format!("{}.{}", table_name, column_name),
			fragment: column.to_owned(),
		}
		.into());
	};

	Ok((t.id, c.id))
}

fn map_cardinality(c: AstRelationshipCardinality) -> RelationshipCardinality {
	match c {
		AstRelationshipCardinality::OneToOne => RelationshipCardinality::OneToOne,
		AstRelationshipCardinality::ManyToOne => RelationshipCardinality::ManyToOne,
		AstRelationshipCardinality::OneToMany => RelationshipCardinality::OneToMany,
		AstRelationshipCardinality::ManyToMany => RelationshipCardinality::ManyToMany,
	}
}
