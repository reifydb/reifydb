// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::shape::RowShape,
	interface::catalog::{
		change::CatalogTrackSeriesChangeOperations,
		id::{ColumnId, NamespaceId, SeriesId},
		property::ColumnPropertyKind,
		series::{Series, SeriesKey, SeriesMetadata},
	},
	internal,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	error,
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId, sumtype::SumTypeId},
};
use tracing::instrument;

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::series::create::{
		SeriesColumnToCreate as StoreSeriesColumnToCreate, SeriesToCreate as StoreSeriesToCreate,
	},
};

#[derive(Debug, Clone)]
pub struct SeriesColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct SeriesToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<SeriesColumnToCreate>,
	pub tag: Option<SumTypeId>,
	pub key: SeriesKey,
	pub underlying: bool,
}

impl From<SeriesColumnToCreate> for StoreSeriesColumnToCreate {
	fn from(col: SeriesColumnToCreate) -> Self {
		StoreSeriesColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			properties: col.properties,
			auto_increment: col.auto_increment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<SeriesToCreate> for StoreSeriesToCreate {
	fn from(to_create: SeriesToCreate) -> Self {
		StoreSeriesToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			tag: to_create.tag,
			key: to_create.key,
			underlying: to_create.underlying,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::series::find", level = "trace", skip(self, txn))]
	pub fn find_series(&self, txn: &mut Transaction<'_>, id: SeriesId) -> Result<Option<Series>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				CatalogStore::find_series(&mut Transaction::Command(&mut *cmd), id)
			}
			Transaction::Admin(admin) => {
				CatalogStore::find_series(&mut Transaction::Admin(&mut *admin), id)
			}
			Transaction::Query(qry) => CatalogStore::find_series(&mut Transaction::Query(&mut *qry), id),
			Transaction::Test(t) => CatalogStore::find_series(&mut Transaction::Admin(&mut *t.inner), id),
			Transaction::Replica(rep) => {
				CatalogStore::find_series(&mut Transaction::Replica(&mut *rep), id)
			}
		}
	}

	#[instrument(name = "catalog::series::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_series_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Series>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				CatalogStore::find_series_by_name(&mut Transaction::Command(&mut *cmd), namespace, name)
			}
			Transaction::Admin(admin) => {
				CatalogStore::find_series_by_name(&mut Transaction::Admin(&mut *admin), namespace, name)
			}
			Transaction::Query(qry) => {
				CatalogStore::find_series_by_name(&mut Transaction::Query(&mut *qry), namespace, name)
			}
			Transaction::Test(t) => CatalogStore::find_series_by_name(
				&mut Transaction::Admin(&mut *t.inner),
				namespace,
				name,
			),
			Transaction::Replica(rep) => {
				CatalogStore::find_series_by_name(&mut Transaction::Replica(&mut *rep), namespace, name)
			}
		}
	}

	#[instrument(name = "catalog::series::get", level = "trace", skip(self, txn))]
	pub fn get_series(&self, txn: &mut Transaction<'_>, id: SeriesId) -> Result<Series> {
		self.find_series(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Series with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::series::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_series(&self, txn: &mut AdminTransaction, to_create: SeriesToCreate) -> Result<Series> {
		let series = CatalogStore::create_series(txn, to_create.into())?;
		txn.track_series_created(series.clone())?;

		let shape = RowShape::from(series.columns.as_slice());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), shape.fields().to_vec())?;

		Ok(series)
	}

	pub fn create_series_with_id(
		&self,
		txn: &mut AdminTransaction,
		series_id: SeriesId,
		to_create: SeriesToCreate,
		column_ids: &[ColumnId],
	) -> Result<Series> {
		let series = CatalogStore::create_series_with_id(txn, series_id, to_create.into(), column_ids)?;
		txn.track_series_created(series.clone())?;

		let shape = RowShape::from(series.columns.as_slice());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), shape.fields().to_vec())?;

		Ok(series)
	}

	#[instrument(name = "catalog::series::drop", level = "debug", skip(self, txn))]
	pub fn drop_series(&self, txn: &mut AdminTransaction, series: Series) -> Result<()> {
		CatalogStore::drop_series(txn, series.id)?;
		txn.track_series_deleted(series)?;
		Ok(())
	}

	#[instrument(name = "catalog::series::list_all", level = "debug", skip(self, txn))]
	pub fn list_series_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Series>> {
		CatalogStore::list_series_all(txn)
	}

	#[instrument(name = "catalog::series::find_metadata", level = "trace", skip(self, txn))]
	pub fn find_series_metadata(&self, txn: &mut Transaction<'_>, id: SeriesId) -> Result<Option<SeriesMetadata>> {
		CatalogStore::find_series_metadata(txn, id)
	}

	#[instrument(name = "catalog::series::update_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_series_metadata_txn(&self, txn: &mut Transaction<'_>, metadata: SeriesMetadata) -> Result<()> {
		CatalogStore::update_series_metadata_txn(txn, metadata)
	}
}
