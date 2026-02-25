// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackSeriesChangeOperations,
		id::{NamespaceId, SeriesId},
		policy::ColumnPolicyKind,
		series::{SeriesDef, SeriesMetadata, TimestampPrecision},
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
	CatalogStore,
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
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct SeriesToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<SeriesColumnToCreate>,
	pub tag: Option<SumTypeId>,
	pub precision: TimestampPrecision,
}

impl From<SeriesColumnToCreate> for StoreSeriesColumnToCreate {
	fn from(col: SeriesColumnToCreate) -> Self {
		StoreSeriesColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			policies: col.policies,
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
			precision: to_create.precision,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::series::find", level = "trace", skip(self, txn))]
	pub fn find_series(&self, txn: &mut Transaction<'_>, id: SeriesId) -> crate::Result<Option<SeriesDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				CatalogStore::find_series(&mut Transaction::Command(&mut *cmd), id)
			}
			Transaction::Admin(admin) => {
				CatalogStore::find_series(&mut Transaction::Admin(&mut *admin), id)
			}
			Transaction::Query(qry) => CatalogStore::find_series(&mut Transaction::Query(&mut *qry), id),
		}
	}

	#[instrument(name = "catalog::series::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_series_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<SeriesDef>> {
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
		}
	}

	#[instrument(name = "catalog::series::get", level = "trace", skip(self, txn))]
	pub fn get_series(&self, txn: &mut Transaction<'_>, id: SeriesId) -> crate::Result<SeriesDef> {
		self.find_series(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Series with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::series::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_series(&self, txn: &mut AdminTransaction, to_create: SeriesToCreate) -> crate::Result<SeriesDef> {
		let series = CatalogStore::create_series(txn, to_create.into())?;
		txn.track_series_def_created(series.clone())?;

		let schema = Schema::from(series.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(series)
	}

	#[instrument(name = "catalog::series::list_all", level = "debug", skip(self, txn))]
	pub fn list_series_all(&self, txn: &mut Transaction<'_>) -> crate::Result<Vec<SeriesDef>> {
		CatalogStore::list_series_all(txn)
	}

	#[instrument(name = "catalog::series::find_metadata", level = "trace", skip(self, txn))]
	pub fn find_series_metadata(
		&self,
		txn: &mut Transaction<'_>,
		id: SeriesId,
	) -> crate::Result<Option<SeriesMetadata>> {
		CatalogStore::find_series_metadata(txn, id)
	}

	#[instrument(name = "catalog::series::update_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_series_metadata_txn(
		&self,
		txn: &mut Transaction<'_>,
		metadata: SeriesMetadata,
	) -> crate::Result<()> {
		CatalogStore::update_series_metadata_txn(txn, metadata)
	}
}
