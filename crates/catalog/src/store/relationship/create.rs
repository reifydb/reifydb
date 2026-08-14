// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, NamespaceId, TableId},
		relationship::{Relationship, RelationshipCardinality, RelationshipJunction},
	},
	key::relationship::RelationshipKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{relationship::shape::relationship as relationship_shape, sequence::system::SystemSequence},
};

#[derive(Debug, Clone)]
pub struct RelationshipToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub source_table: TableId,
	pub source_column: ColumnId,
	pub target_table: TableId,
	pub target_column: ColumnId,
	pub junction: Option<RelationshipJunction>,
	pub cardinality: RelationshipCardinality,
}

impl CatalogStore {
	pub(crate) fn create_relationship(
		txn: &mut AdminTransaction,
		to_create: RelationshipToCreate,
	) -> Result<Relationship> {
		if to_create.cardinality.requires_junction() != to_create.junction.is_some() {
			return Err(CatalogError::RelationshipJunctionMismatch {
				fragment: to_create.name.clone(),
				cardinality: to_create.cardinality.as_str().to_string(),
				has_junction: to_create.junction.is_some(),
			}
			.into());
		}

		if let Some(_existing) = CatalogStore::find_relationship_by_name(
			&mut Transaction::Admin(&mut *txn),
			to_create.namespace,
			to_create.source_table,
			to_create.name.text(),
		)? {
			let namespace =
				CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), to_create.namespace)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Relationship,
				namespace: namespace.name().to_string(),
				name: to_create.name.text().to_string(),
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let id = SystemSequence::next_relationship_id(txn)?;

		let mut row = relationship_shape::allocate();
		relationship_shape::set_id(&mut row, id.0);
		relationship_shape::set_namespace_id(&mut row, to_create.namespace.0);
		relationship_shape::set_name(&mut row, to_create.name.text());
		relationship_shape::set_source_table_id(&mut row, to_create.source_table.0);
		relationship_shape::set_source_column_id(&mut row, to_create.source_column.0);
		relationship_shape::set_target_table_id(&mut row, to_create.target_table.0);
		relationship_shape::set_target_column_id(&mut row, to_create.target_column.0);

		let (junction_table, junction_source_col, junction_target_col) = match &to_create.junction {
			Some(j) => (j.table.0, j.source_column.0, j.target_column.0),
			None => (0, 0, 0),
		};
		relationship_shape::set_junction_table_id(&mut row, junction_table);
		relationship_shape::set_junction_source_column_id(&mut row, junction_source_col);
		relationship_shape::set_junction_target_column_id(&mut row, junction_target_col);
		relationship_shape::set_cardinality(&mut row, to_create.cardinality.as_code());

		txn.set(&RelationshipKey::encoded(id), row.freeze())?;

		Ok(Relationship {
			id,
			namespace: to_create.namespace,
			name: to_create.name.text().to_string(),
			source_table: to_create.source_table,
			source_column: to_create.source_column,
			target_table: to_create.target_table,
			target_column: to_create.target_column,
			junction: to_create.junction,
			cardinality: to_create.cardinality,
		})
	}

	pub(crate) fn find_relationship_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		source_table: TableId,
		name: &str,
	) -> Result<Option<Relationship>> {
		for rel in Self::list_relationships(rx)? {
			if rel.namespace == namespace && rel.source_table == source_table && rel.name == name {
				return Ok(Some(rel));
			}
		}
		Ok(None)
	}
}
