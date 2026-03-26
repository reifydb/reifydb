// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackRingBufferChangeOperations,
		id::{NamespaceId, PrimaryKeyId, RingBufferId},
		property::ColumnPropertyKind,
		ringbuffer::{PartitionedMetadata, RingBuffer, RingBufferMetadata},
	},
	internal,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction};
use reifydb_type::{
	error,
	fragment::Fragment,
	value::{Value, constraint::TypeConstraint, dictionary::DictionaryId},
};
use tracing::instrument;

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::ringbuffer::create::{
		RingBufferColumnToCreate as StoreRingBufferColumnToCreate,
		RingBufferToCreate as StoreRingBufferToCreate,
	},
};

#[derive(Debug, Clone)]
pub struct RingBufferColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct RingBufferToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
	pub partition_by: Vec<String>,
}

impl From<RingBufferColumnToCreate> for StoreRingBufferColumnToCreate {
	fn from(col: RingBufferColumnToCreate) -> Self {
		StoreRingBufferColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			properties: col.properties,
			auto_increment: col.auto_increment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<RingBufferToCreate> for StoreRingBufferToCreate {
	fn from(to_create: RingBufferToCreate) -> Self {
		StoreRingBufferToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			capacity: to_create.capacity,
			partition_by: to_create.partition_by,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::ringbuffer::find", level = "trace", skip(self, txn))]
	pub fn find_ringbuffer(&self, txn: &mut Transaction<'_>, id: RingBufferId) -> Result<Option<RingBuffer>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				CatalogStore::find_ringbuffer(&mut Transaction::Command(&mut *cmd), id)
			}
			Transaction::Admin(admin) => {
				CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut *admin), id)
			}
			Transaction::Query(qry) => {
				CatalogStore::find_ringbuffer(&mut Transaction::Query(&mut *qry), id)
			}
			Transaction::Subscription(sub) => {
				CatalogStore::find_ringbuffer(&mut Transaction::Subscription(&mut *sub), id)
			}
			Transaction::Test(t) => {
				CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut *t.inner), id)
			}
		}
	}

	#[instrument(name = "catalog::ringbuffer::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_ringbuffer_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<RingBuffer>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => CatalogStore::find_ringbuffer_by_name(
				&mut Transaction::Command(&mut *cmd),
				namespace,
				name,
			),
			Transaction::Admin(admin) => CatalogStore::find_ringbuffer_by_name(
				&mut Transaction::Admin(&mut *admin),
				namespace,
				name,
			),
			Transaction::Query(qry) => CatalogStore::find_ringbuffer_by_name(
				&mut Transaction::Query(&mut *qry),
				namespace,
				name,
			),
			Transaction::Subscription(sub) => CatalogStore::find_ringbuffer_by_name(
				&mut Transaction::Subscription(&mut *sub),
				namespace,
				name,
			),
			Transaction::Test(t) => CatalogStore::find_ringbuffer_by_name(
				&mut Transaction::Admin(&mut *t.inner),
				namespace,
				name,
			),
		}
	}

	#[instrument(name = "catalog::ringbuffer::get", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer(&self, txn: &mut Transaction<'_>, id: RingBufferId) -> Result<RingBuffer> {
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
	) -> Result<RingBuffer> {
		let ringbuffer = CatalogStore::create_ringbuffer(txn, to_create.into())?;
		txn.track_ringbuffer_created(ringbuffer.clone())?;

		let schema = Schema::from(ringbuffer.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(ringbuffer)
	}

	#[instrument(name = "catalog::ringbuffer::drop", level = "debug", skip(self, txn))]
	pub fn drop_ringbuffer(&self, txn: &mut AdminTransaction, ringbuffer: RingBuffer) -> Result<()> {
		CatalogStore::drop_ringbuffer(txn, ringbuffer.id)?;
		txn.track_ringbuffer_deleted(ringbuffer)?;
		Ok(())
	}

	#[instrument(name = "catalog::ringbuffer::list_all", level = "debug", skip(self, txn))]
	pub fn list_ringbuffers_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<RingBuffer>> {
		CatalogStore::list_ringbuffers_all(txn)
	}

	#[instrument(name = "catalog::ringbuffer::find_metadata", level = "trace", skip(self, txn))]
	pub fn find_ringbuffer_metadata(
		&self,
		txn: &mut Transaction<'_>,
		id: RingBufferId,
	) -> Result<Option<RingBufferMetadata>> {
		CatalogStore::find_ringbuffer_metadata(txn, id)
	}

	#[instrument(name = "catalog::ringbuffer::get_metadata", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer_metadata(
		&self,
		txn: &mut Transaction<'_>,
		id: RingBufferId,
	) -> Result<RingBufferMetadata> {
		CatalogStore::get_ringbuffer_metadata(txn, id)
	}

	#[instrument(name = "catalog::ringbuffer::update_metadata", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_metadata(
		&self,
		txn: &mut CommandTransaction,
		metadata: RingBufferMetadata,
	) -> Result<()> {
		CatalogStore::update_ringbuffer_metadata(txn, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::update_metadata_admin", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_metadata_admin(
		&self,
		txn: &mut AdminTransaction,
		metadata: RingBufferMetadata,
	) -> Result<()> {
		CatalogStore::update_ringbuffer_metadata_admin(txn, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::update_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_metadata_txn(
		&self,
		txn: &mut Transaction<'_>,
		metadata: RingBufferMetadata,
	) -> Result<()> {
		CatalogStore::update_ringbuffer_metadata_txn(txn, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::find_partition_metadata", level = "trace", skip(self, txn))]
	pub fn find_ringbuffer_partition_metadata(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
		partition_values: &[Value],
	) -> Result<Option<RingBufferMetadata>> {
		CatalogStore::find_ringbuffer_partition_metadata(txn, ringbuffer, partition_values)
	}

	#[instrument(name = "catalog::ringbuffer::list_partition_metadata", level = "trace", skip(self, txn))]
	pub fn list_ringbuffer_partition_metadata(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
	) -> Result<Vec<PartitionedMetadata>> {
		CatalogStore::list_ringbuffer_partition_metadata(txn, ringbuffer)
	}

	#[instrument(name = "catalog::ringbuffer::update_partition_metadata", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_partition_metadata(
		&self,
		txn: &mut CommandTransaction,
		ringbuffer: RingBufferId,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		CatalogStore::update_ringbuffer_partition_metadata(txn, ringbuffer, partition_values, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::update_partition_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_ringbuffer_partition_metadata_txn(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		CatalogStore::update_ringbuffer_partition_metadata_txn(txn, ringbuffer, partition_values, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::list_partitions", level = "trace", skip(self, txn))]
	pub fn list_ringbuffer_partitions(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
	) -> Result<Vec<PartitionedMetadata>> {
		CatalogStore::list_ringbuffer_partitions(txn, ringbuffer)
	}

	#[instrument(name = "catalog::ringbuffer::find_partition_metadata", level = "trace", skip(self, txn))]
	pub fn find_partition_metadata(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition_key: &[Value],
	) -> Result<Option<RingBufferMetadata>> {
		CatalogStore::find_partition_metadata(txn, ringbuffer, partition_key)
	}

	#[instrument(name = "catalog::ringbuffer::save_partition_metadata", level = "debug", skip(self, txn))]
	pub fn save_partition_metadata(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition_key: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		CatalogStore::save_partition_metadata(txn, ringbuffer, partition_key, metadata)
	}

	#[instrument(name = "catalog::ringbuffer::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_ringbuffer_primary_key(
		&self,
		txn: &mut AdminTransaction,
		ringbuffer_id: RingBufferId,
		primary_key_id: PrimaryKeyId,
	) -> Result<()> {
		CatalogStore::set_ringbuffer_primary_key(txn, ringbuffer_id, primary_key_id)
	}

	#[instrument(name = "catalog::ringbuffer::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer_pk_id(
		&self,
		txn: &mut Transaction<'_>,
		ringbuffer_id: RingBufferId,
	) -> Result<Option<PrimaryKeyId>> {
		CatalogStore::get_ringbuffer_pk_id(txn, ringbuffer_id)
	}
}
