// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		id::{NamespaceId, ViewId},
		ringbuffer::{RingBufferMetadata, encode_ringbuffer_metadata},
		series::SeriesKey,
		storage::StorageId,
		view::{
			View, ViewKind,
			ViewKind::{Deferred, Transactional},
			ViewSortKey, ViewStorageKind,
		},
	},
	key::{catalog::ViewKey, namespace::NamespaceViewKey, ringbuffer::RingBufferMetadataKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId, sumtype::SumTypeId},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		sequence::system::SystemSequence,
		view::shape::{encode_view_sort, view, view_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct ViewColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub enum ViewStorage {
	Table {
		partition_by: Vec<String>,
	},
	RingBuffer {
		capacity: u64,
		partition_by: Vec<String>,
	},
	Series {
		key: SeriesKey,
		tag: Option<SumTypeId>,
		partition_by: Vec<String>,
	},
}

impl ViewStorage {
	pub fn partition_by(&self) -> &[String] {
		match self {
			ViewStorage::Table {
				partition_by,
			} => partition_by,
			ViewStorage::RingBuffer {
				partition_by,
				..
			} => partition_by,
			ViewStorage::Series {
				partition_by,
				..
			} => partition_by,
		}
	}
}

impl Default for ViewStorage {
	fn default() -> Self {
		ViewStorage::Table {
			partition_by: Vec::new(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct ViewToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<ViewColumnToCreate>,
	pub storage: ViewStorage,
	pub sort: Vec<ViewSortKey>,
}

impl CatalogStore {
	pub(crate) fn create_deferred_view(txn: &mut AdminTransaction, to_create: ViewToCreate) -> Result<View> {
		Self::create_view(txn, to_create, Deferred)
	}

	pub(crate) fn create_transactional_view(txn: &mut AdminTransaction, to_create: ViewToCreate) -> Result<View> {
		Self::create_view(txn, to_create, Transactional)
	}

	fn create_view(txn: &mut AdminTransaction, to_create: ViewToCreate, kind: ViewKind) -> Result<View> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_view(txn, namespace_id, &to_create.name)?;

		let view_id = SystemSequence::next_view_id(txn)?;
		Self::store_view(txn, view_id, namespace_id, &to_create, kind)?;
		Self::link_view_to_namespace(txn, namespace_id, view_id, to_create.name.text())?;
		Self::initialize_view_family_metadata(txn, view_id, &to_create.storage)?;

		Self::insert_columns_for_view(txn, view_id, to_create)?;

		Self::get_view(&mut Transaction::Admin(&mut *txn), view_id)
	}

	#[inline]
	fn reject_existing_view(txn: &mut AdminTransaction, namespace_id: NamespaceId, name: &Fragment) -> Result<()> {
		let Some(view) =
			CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut *txn), namespace_id, name.text())?
		else {
			return Ok(());
		};
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::View,
			namespace: namespace.name().to_string(),
			name: view.name().to_string(),
			fragment: name.clone(),
		}
		.into())
	}

	fn store_view(
		txn: &mut AdminTransaction,
		view: ViewId,
		namespace: NamespaceId,
		to_create: &ViewToCreate,
		kind: ViewKind,
	) -> Result<()> {
		let mut row = view::allocate();
		view::set_id(&mut row, u64::from(view));
		view::set_namespace(&mut row, u64::from(namespace));
		view::set_name(&mut row, to_create.name.text());
		view::set_kind(
			&mut row,
			match kind {
				Deferred => 0,
				Transactional => 1,
			},
		);
		view::set_primary_key(&mut row, 0u64);
		view::set_sort(&mut row, encode_view_sort(&to_create.sort));
		view::set_partition_by(&mut row, to_create.storage.partition_by().join(","));

		match &to_create.storage {
			ViewStorage::Table {
				..
			} => {
				view::set_storage_kind(&mut row, ViewStorageKind::Table as u8);
				view::set_capacity(&mut row, 0u64);
				view::set_key_column(&mut row, "");
				view::set_key_kind(&mut row, 0u8);
				view::set_precision(&mut row, 0u8);
				view::set_tag_id(&mut row, 0u64);
			}
			ViewStorage::RingBuffer {
				capacity,
				..
			} => {
				view::set_storage_kind(&mut row, ViewStorageKind::RingBuffer as u8);
				view::set_capacity(&mut row, *capacity);
				view::set_key_column(&mut row, "");
				view::set_key_kind(&mut row, 0u8);
				view::set_precision(&mut row, 0u8);
				view::set_tag_id(&mut row, 0u64);
			}
			ViewStorage::Series {
				key,
				tag,
				..
			} => {
				view::set_storage_kind(&mut row, ViewStorageKind::Series as u8);
				view::set_capacity(&mut row, 0u64);
				view::set_key_column(&mut row, key.column());
				let (key_kind_u8, precision_u8) = match key {
					SeriesKey::DateTime {
						precision,
						..
					} => (0u8, *precision as u8),
					SeriesKey::Integer {
						..
					} => (1u8, 0u8),
				};
				view::set_key_kind(&mut row, key_kind_u8);
				view::set_precision(&mut row, precision_u8);
				view::set_tag_id(&mut row, tag.map(|t| t.0).unwrap_or(0));
			}
		}

		txn.set(&ViewKey::encoded(view), row.freeze())?;

		Ok(())
	}

	fn initialize_view_family_metadata(
		txn: &mut AdminTransaction,
		view: ViewId,
		storage: &ViewStorage,
	) -> Result<()> {
		match storage {
			ViewStorage::Table {
				..
			} => Ok(()),
			ViewStorage::RingBuffer {
				partition_by,
				..
			} => {
				if partition_by.is_empty() {
					let row = encode_ringbuffer_metadata(&RingBufferMetadata::new());
					txn.set(
						&RingBufferMetadataKey::encoded(StorageId::View(view)),
						row.into_bytes(),
					)?;
				}
				Ok(())
			}
			ViewStorage::Series {
				..
			} => Ok(()),
		}
	}

	fn link_view_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		view: ViewId,
		name: &str,
	) -> Result<()> {
		let mut row = view_namespace::allocate();
		view_namespace::set_id(&mut row, u64::from(view));
		view_namespace::set_name(&mut row, name);
		txn.set(&NamespaceViewKey::encoded(namespace, view), row.freeze())?;
		Ok(())
	}

	fn insert_columns_for_view(txn: &mut AdminTransaction, view: ViewId, to_create: ViewToCreate) -> Result<()> {
		let namespace = Self::get_namespace(&mut Transaction::Admin(&mut *txn), to_create.namespace)?;

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				view,
				ColumnToCreate {
					fragment: Some(column_to_create.fragment.clone()),
					namespace_name: namespace.name().to_string(),
					object_name: to_create.name.text().to_string(),
					column: column_to_create.name.text().to_string(),
					constraint: column_to_create.constraint.clone(),
					properties: vec![],
					index: ColumnIndex(idx as u8),
					auto_increment: false,
					dictionary_id: column_to_create.dictionary_id,
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_core::{
		interface::catalog::id::{NamespaceId, ViewId},
		key::namespace::NamespaceViewKey,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::fragment::Fragment;

	use super::ViewStorage;
	use crate::{
		CatalogStore,
		store::view::{create::ViewToCreate, shape::view_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_deferred_view() {
		let mut txn = create_test_admin_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let to_create = ViewToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("test_view"),
			columns: vec![],
			storage: ViewStorage::default(),
			sort: vec![],
		};

		let result = CatalogStore::create_deferred_view(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id(), ViewId(16385));
		assert_eq!(result.namespace(), NamespaceId(16385));
		assert_eq!(result.name(), "test_view");

		let err = CatalogStore::create_deferred_view(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_view_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = ViewToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("test_view"),
			columns: vec![],
			storage: ViewStorage::default(),
			sort: vec![],
		};

		CatalogStore::create_deferred_view(&mut txn, to_create).unwrap();

		let to_create = ViewToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("another_view"),
			columns: vec![],
			storage: ViewStorage::default(),
			sort: vec![],
		};

		CatalogStore::create_deferred_view(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceViewKey::full_scan(namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let bytes = &link.bytes;
		assert_eq!(view_namespace::get_id(EncodedCatalogRow::view(bytes)), 16385);
		assert_eq!(view_namespace::get_name(EncodedCatalogRow::view(bytes)), "test_view");

		let link = &links[0];
		let bytes = &link.bytes;
		assert_eq!(view_namespace::get_id(EncodedCatalogRow::view(bytes)), 16386);
		assert_eq!(view_namespace::get_name(EncodedCatalogRow::view(bytes)), "another_view");
	}

	#[test]
	fn test_create_deferred_view_missing_namespace() {
		let mut txn = create_test_admin_transaction();

		let to_create = ViewToCreate {
			namespace: NamespaceId(999), // Non-existent namespace
			name: Fragment::internal("my_view"),
			columns: vec![],
			storage: ViewStorage::default(),
			sort: vec![],
		};

		CatalogStore::create_deferred_view(&mut txn, to_create).unwrap_err();
	}
}
