// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackRelationshipChangeOperations,
	id::{ColumnId, NamespaceId, RelationshipId, TableId},
	relationship::{Relationship, RelationshipCardinality, RelationshipJunction},
};
use reifydb_transaction::{
	change::TransactionalRelationshipChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
	store::relationship::create::RelationshipToCreate as StoreRelationshipToCreate,
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

impl From<RelationshipToCreate> for StoreRelationshipToCreate {
	fn from(value: RelationshipToCreate) -> Self {
		StoreRelationshipToCreate {
			name: value.name,
			namespace: value.namespace,
			source_table: value.source_table,
			source_column: value.source_column,
			target_table: value.target_table,
			target_column: value.target_column,
			junction: value.junction,
			cardinality: value.cardinality,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::relationship::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_relationship(
		&self,
		txn: &mut AdminTransaction,
		to_create: RelationshipToCreate,
	) -> Result<Relationship> {
		let rel = CatalogStore::create_relationship(txn, to_create.into())?;
		txn.track_relationship_created(rel.clone())?;
		Ok(rel)
	}

	#[instrument(name = "catalog::relationship::drop", level = "debug", skip(self, txn))]
	pub fn drop_relationship(
		&self,
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		source_table: TableId,
		name: &str,
	) -> Result<()> {
		let rel = match self.find_relationship_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace,
			source_table,
			name,
		)? {
			Some(rel) => rel,
			None => {
				let ns = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace)?;
				return Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Relationship,
					namespace: ns.name().to_string(),
					name: name.to_string(),
					fragment: Fragment::None,
				}
				.into());
			}
		};
		CatalogStore::drop_relationship(txn, rel.id)?;
		txn.track_relationship_deleted(rel)?;
		Ok(())
	}

	#[instrument(name = "catalog::relationship::find", level = "trace", skip(self, txn))]
	pub fn find_relationship(&self, txn: &mut Transaction<'_>, id: RelationshipId) -> Result<Option<Relationship>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(rel) = self.materialized.find_relationship_at(id, cmd.version()) {
					return Ok(Some(rel));
				}
				if let Some(rel) =
					CatalogStore::find_relationship(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!(
						"Relationship with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(rel) = TransactionalRelationshipChanges::find_relationship(admin, id) {
					return Ok(Some(rel.clone()));
				}
				if TransactionalRelationshipChanges::is_relationship_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(rel) = self.materialized.find_relationship_at(id, admin.version()) {
					return Ok(Some(rel));
				}
				if let Some(rel) =
					CatalogStore::find_relationship(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!(
						"Relationship with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(rel) = self.materialized.find_relationship_at(id, qry.version()) {
					return Ok(Some(rel));
				}
				if let Some(rel) =
					CatalogStore::find_relationship(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!(
						"Relationship with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(rel) = TransactionalRelationshipChanges::find_relationship(t.inner, id) {
					return Ok(Some(rel.clone()));
				}
				if TransactionalRelationshipChanges::is_relationship_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(rel) = CatalogStore::find_relationship(
					&mut Transaction::Test(Box::new(t.reborrow())),
					id,
				)? {
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(rel) = self.materialized.find_relationship_at(id, rep.version()) {
					return Ok(Some(rel));
				}
				if let Some(rel) =
					CatalogStore::find_relationship(&mut Transaction::Replica(&mut *rep), id)?
				{
					warn!(
						"Relationship with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::relationship::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_relationship_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		source_table: TableId,
		name: &str,
	) -> Result<Option<Relationship>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(rel) = self.materialized.find_relationship_by_name_at(
					namespace,
					source_table,
					name,
					cmd.version(),
				) {
					return Ok(Some(rel));
				}
				if let Some(rel) = CatalogStore::find_relationship_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					source_table,
					name,
				)? {
					warn!(
						"Relationship '{}' for source_table {:?} in namespace {:?} found in storage but not in MaterializedCatalog",
						name, source_table, namespace
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(rel) = TransactionalRelationshipChanges::find_relationship_by_name(
					admin,
					namespace,
					source_table,
					name,
				) {
					return Ok(Some(rel.clone()));
				}
				if TransactionalRelationshipChanges::is_relationship_deleted_by_name(
					admin,
					namespace,
					source_table,
					name,
				) {
					return Ok(None);
				}
				if let Some(rel) = self.materialized.find_relationship_by_name_at(
					namespace,
					source_table,
					name,
					admin.version(),
				) {
					return Ok(Some(rel));
				}
				if let Some(rel) = CatalogStore::find_relationship_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					source_table,
					name,
				)? {
					warn!(
						"Relationship '{}' for source_table {:?} in namespace {:?} found in storage but not in MaterializedCatalog",
						name, source_table, namespace
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(rel) = self.materialized.find_relationship_by_name_at(
					namespace,
					source_table,
					name,
					qry.version(),
				) {
					return Ok(Some(rel));
				}
				if let Some(rel) = CatalogStore::find_relationship_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					source_table,
					name,
				)? {
					warn!(
						"Relationship '{}' for source_table {:?} in namespace {:?} found in storage but not in MaterializedCatalog",
						name, source_table, namespace
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(rel) = TransactionalRelationshipChanges::find_relationship_by_name(
					t.inner,
					namespace,
					source_table,
					name,
				) {
					return Ok(Some(rel.clone()));
				}
				if TransactionalRelationshipChanges::is_relationship_deleted_by_name(
					t.inner,
					namespace,
					source_table,
					name,
				) {
					return Ok(None);
				}
				if let Some(rel) = CatalogStore::find_relationship_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					source_table,
					name,
				)? {
					return Ok(Some(rel));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(rel) = self.materialized.find_relationship_by_name_at(
					namespace,
					source_table,
					name,
					rep.version(),
				) {
					return Ok(Some(rel));
				}
				if let Some(rel) = CatalogStore::find_relationship_by_name(
					&mut Transaction::Replica(&mut *rep),
					namespace,
					source_table,
					name,
				)? {
					warn!(
						"Relationship '{}' for source_table {:?} in namespace {:?} found in storage but not in MaterializedCatalog",
						name, source_table, namespace
					);
					return Ok(Some(rel));
				}
				Ok(None)
			}
		}
	}

	/// Lists all relationships in the catalog. This is a cold path that goes directly to storage;
	/// it does not consult MaterializedCatalog or transactional changes (a global list is rarely
	/// needed and merging tx-pending changes here would duplicate logic that callers can do).
	#[instrument(name = "catalog::relationship::list", level = "debug", skip(self, txn))]
	pub fn list_relationships(&self, txn: &mut Transaction<'_>) -> Result<Vec<Relationship>> {
		CatalogStore::list_relationships(txn)
	}

	#[instrument(name = "catalog::relationship::list_from", level = "debug", skip(self, txn))]
	pub fn list_relationships_from(
		&self,
		txn: &mut Transaction<'_>,
		source_table: TableId,
	) -> Result<Vec<Relationship>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_relationships_from_at(source_table, cmd.version()))
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_relationships_from_at(source_table, qry.version()))
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_relationships_from_at(source_table, rep.version()))
			}
			Transaction::Admin(admin) => {
				let mut base =
					self.materialized.list_relationships_from_at(source_table, admin.version());

				for change in admin.changes.relationship.iter() {
					match (&change.pre, &change.post) {
						(_, Some(post)) if post.source_table == source_table => {
							base.retain(|r| r.id != post.id);
							base.push(post.clone());
						}
						(Some(pre), None) if pre.source_table == source_table => {
							base.retain(|r| r.id != pre.id);
						}
						_ => {}
					}
				}
				Ok(base)
			}
			Transaction::Test(mut t) => {
				let mut base =
					self.materialized.list_relationships_from_at(source_table, t.inner.version());

				for change in t.inner.changes.relationship.iter() {
					match (&change.pre, &change.post) {
						(_, Some(post)) if post.source_table == source_table => {
							base.retain(|r| r.id != post.id);
							base.push(post.clone());
						}
						(Some(pre), None) if pre.source_table == source_table => {
							base.retain(|r| r.id != pre.id);
						}
						_ => {}
					}
				}
				let _ = t.reborrow();
				Ok(base)
			}
		}
	}
}
