// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackRingBufferChangeOperations,
		id::{NamespaceId, PrimaryKeyId, RingBufferId},
		policy::ColumnPolicyKind,
		ringbuffer::{RingBufferDef, RingBufferMetadata},
	},
	internal,
};
use reifydb_transaction::transaction::{
	AsTransaction, Transaction, admin::AdminTransaction, command::CommandTransaction,
};
use reifydb_type::{
	error,
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};
use tracing::instrument;

use crate::{
	CatalogStore,
	catalog::Catalog,
	store::ringbuffer::create::{
		RingBufferColumnToCreate as StoreRingBufferColumnToCreate,
		RingBufferToCreate as StoreRingBufferToCreate,
	},
};

#[derive(Debug, Clone)]
pub struct RingBufferColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<Fragment>,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct RingBufferToCreate {
	pub fragment: Option<Fragment>,
	pub ringbuffer: String,
	pub namespace: NamespaceId,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

impl From<RingBufferColumnToCreate> for StoreRingBufferColumnToCreate {
	fn from(col: RingBufferColumnToCreate) -> Self {
		StoreRingBufferColumnToCreate {
			name: col.name,
			constraint: col.constraint,
			policies: col.policies,
			auto_increment: col.auto_increment,
			fragment: col.fragment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<RingBufferToCreate> for StoreRingBufferToCreate {
	fn from(to_create: RingBufferToCreate) -> Self {
		StoreRingBufferToCreate {
			fragment: to_create.fragment,
			ringbuffer: to_create.ringbuffer,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			capacity: to_create.capacity,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::ringbuffer::find", level = "trace", skip(self, txn))]
	pub fn find_ringbuffer<T: AsTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>> {
		match txn.as_transaction() {
			Transaction::Command(cmd) => CatalogStore::find_ringbuffer(cmd, id),
			Transaction::Admin(admin) => CatalogStore::find_ringbuffer(admin, id),
			Transaction::Query(qry) => CatalogStore::find_ringbuffer(qry, id),
		}
	}

	#[instrument(name = "catalog::ringbuffer::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_ringbuffer_by_name<T: AsTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<RingBufferDef>> {
		match txn.as_transaction() {
			Transaction::Command(cmd) => CatalogStore::find_ringbuffer_by_name(cmd, namespace, name),
			Transaction::Admin(admin) => CatalogStore::find_ringbuffer_by_name(admin, namespace, name),
			Transaction::Query(qry) => CatalogStore::find_ringbuffer_by_name(qry, namespace, name),
		}
	}

	#[instrument(name = "catalog::ringbuffer::get", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer<T: AsTransaction>(&self, txn: &mut T, id: RingBufferId) -> crate::Result<RingBufferDef> {
		self.find_ringbuffer(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"RingBuffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::ringbuffer::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_ringbuffer(
		&self,
		txn: &mut AdminTransaction,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef> {
		let ringbuffer = CatalogStore::create_ringbuffer(txn, to_create.into())?;
		txn.track_ringbuffer_def_created(ringbuffer.clone())?;

		let schema = Schema::from(ringbuffer.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(ringbuffer)
	}

	#[instrument(name = "catalog::ringbuffer::list_all", level = "debug", skip(self, txn))]
	pub fn list_ringbuffers_all<T: AsTransaction>(&self, txn: &mut T) -> crate::Result<Vec<RingBufferDef>> {
		CatalogStore::list_ringbuffers_all(txn)
	}

	#[instrument(name = "catalog::ringbuffer::find_metadata", level = "trace", skip(self, txn))]
	pub fn find_ringbuffer_metadata<T: AsTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<Option<RingBufferMetadata>> {
		CatalogStore::find_ringbuffer_metadata(txn, id)
	}

	#[instrument(name = "catalog::ringbuffer::get_metadata", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer_metadata<T: AsTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<RingBufferMetadata> {
		CatalogStore::get_ringbuffer_metadata(txn, id)
	}

	#[instrument(name = "catalog::ringbuffer::update_metadata", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_metadata(
		&self,
		txn: &mut CommandTransaction,
		metadata: RingBufferMetadata,
	) -> crate::Result<()> {
		CatalogStore::update_ringbuffer_metadata(txn, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::update_metadata_admin", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_metadata_admin(
		&self,
		txn: &mut AdminTransaction,
		metadata: RingBufferMetadata,
	) -> crate::Result<()> {
		CatalogStore::update_ringbuffer_metadata_admin(txn, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::update_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_metadata_txn(
		&self,
		txn: &mut Transaction<'_>,
		metadata: RingBufferMetadata,
	) -> crate::Result<()> {
		CatalogStore::update_ringbuffer_metadata_txn(txn, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_ringbuffer_primary_key(
		&self,
		txn: &mut AdminTransaction,
		ringbuffer_id: RingBufferId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		CatalogStore::set_ringbuffer_primary_key(txn, ringbuffer_id, primary_key_id)
	}

	#[instrument(name = "catalog::ringbuffer::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer_pk_id<T: AsTransaction>(
		&self,
		txn: &mut T,
		ringbuffer_id: RingBufferId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		CatalogStore::get_ringbuffer_pk_id(txn, ringbuffer_id)
	}
}
