// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		change::CatalogTrackSeriesChangeOperations,
		id::{ColumnId, NamespaceId, SeriesId},
		property::ColumnPropertyKind,
		series::{Series, SeriesEncoding, SeriesKey, SeriesPartitionMetadata},
	},
	internal,
};
use reifydb_transaction::{
	change::TransactionalSeriesChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{
		constraint::TypeConstraint, dictionary::DictionaryId, partition::Partition, sumtype::SumTypeId,
		value_type::ValueType,
	},
};
use tracing::{instrument, warn};

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
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct SeriesToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<SeriesColumnToCreate>,
	pub tag: Option<SumTypeId>,
	pub key: SeriesKey,
	pub partition_by: Vec<String>,
	pub time: TimeSource,
	pub encoding: Option<SeriesEncoding>,
}

impl From<SeriesColumnToCreate> for StoreSeriesColumnToCreate {
	fn from(col: SeriesColumnToCreate) -> Self {
		StoreSeriesColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			properties: col.properties,
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
			partition_by: to_create.partition_by,
			time: to_create.time,
			encoding: to_create.encoding,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::series::find", level = "trace", skip(self, txn))]
	pub fn find_series(&self, txn: &mut Transaction<'_>, id: SeriesId) -> Result<Option<Series>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(series) = self.cache.find_series_at(id, cmd.version()) {
					return Ok(Some(series));
				}
				if let Some(series) =
					CatalogStore::find_series(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!("Series {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(series));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(series) = TransactionalSeriesChanges::find_series(admin, id) {
					return Ok(Some(series.clone()));
				}
				if TransactionalSeriesChanges::is_series_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(series) = self.cache.find_series_at(id, admin.version()) {
					return Ok(Some(series));
				}
				if let Some(series) =
					CatalogStore::find_series(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("Series {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(series));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(series) = self.cache.find_series_at(id, qry.version()) {
					return Ok(Some(series));
				}
				if let Some(series) = CatalogStore::find_series(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!("Series {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(series));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(series) = TransactionalSeriesChanges::find_series(t.inner, id) {
					return Ok(Some(series.clone()));
				}
				if TransactionalSeriesChanges::is_series_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(series) =
					CatalogStore::find_series(&mut Transaction::Admin(&mut *t.inner), id)?
				{
					return Ok(Some(series));
				}
				Ok(None)
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
				if let Some(series) = self.cache.find_series_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(series));
				}
				if let Some(series) = CatalogStore::find_series_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Series '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(series));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(series) =
					TransactionalSeriesChanges::find_series_by_name(admin, namespace, name)
				{
					return Ok(Some(series.clone()));
				}
				if TransactionalSeriesChanges::is_series_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}
				if let Some(series) =
					self.cache.find_series_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(series));
				}
				if let Some(series) = CatalogStore::find_series_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Series '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(series));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(series) = self.cache.find_series_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(series));
				}
				if let Some(series) = CatalogStore::find_series_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"Series '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(series));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(series) =
					TransactionalSeriesChanges::find_series_by_name(t.inner, namespace, name)
				{
					return Ok(Some(series.clone()));
				}
				if TransactionalSeriesChanges::is_series_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(series) = CatalogStore::find_series_by_name(
					&mut Transaction::Admin(&mut *t.inner),
					namespace,
					name,
				)? {
					return Ok(Some(series));
				}
				Ok(None)
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

	pub fn get_or_create_series_shape(&self, txn: &mut Transaction<'_>, series: &Series) -> Result<RowShape> {
		let fields = self.series_shape_fields(txn, series)?;
		self.get_or_create_row_shape(txn, RowFamily::Series, fields)
	}

	pub fn find_series_shape(&self, txn: &mut Transaction<'_>, series: &Series) -> Result<Option<RowShape>> {
		let fields = self.series_shape_fields(txn, series)?;
		self.get_or_load_row_shape(RowShape::new(RowFamily::Series, fields).fingerprint(), txn)
	}

	fn series_shape_fields(&self, txn: &mut Transaction<'_>, series: &Series) -> Result<Vec<RowShapeField>> {
		let mut fields = Vec::with_capacity(1 + series.columns.len());

		let key_column = series.key.column();
		let key_col = series.columns.iter().find(|c| c.name == key_column);
		let key_type = key_col
			.map(|c| c.constraint.clone())
			.unwrap_or_else(|| TypeConstraint::unconstrained(ValueType::Int8));
		fields.push(RowShapeField::new(key_column.to_string(), key_type));
		for col in series.data_columns() {
			let constraint = if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = self.find_dictionary(txn, dict_id)? {
					TypeConstraint::dictionary(dict_id, dict.id_type)
				} else {
					col.constraint.clone()
				}
			} else {
				col.constraint.clone()
			};
			fields.push(RowShapeField::new(col.name.clone(), constraint));
		}
		Ok(fields)
	}

	#[instrument(name = "catalog::series::create", level = "info", skip(self, txn, to_create))]
	pub fn create_series(&self, txn: &mut AdminTransaction, to_create: SeriesToCreate) -> Result<Series> {
		let series = CatalogStore::create_series(txn, to_create.into())?;
		txn.track_series_created(series.clone())?;

		self.get_or_create_series_shape(&mut Transaction::Admin(&mut *txn), &series)?;

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

		self.get_or_create_series_shape(&mut Transaction::Admin(&mut *txn), &series)?;

		Ok(series)
	}

	#[instrument(name = "catalog::series::drop", level = "info", skip(self, txn))]
	pub fn drop_series(&self, txn: &mut AdminTransaction, series: Series) -> Result<()> {
		for block in CatalogStore::list_column_blocks_for_series(&mut Transaction::Admin(&mut *txn), series.id)?
		{
			self.drop_column_block(txn, block.id)?;
		}
		CatalogStore::drop_series(txn, series.id)?;
		txn.track_series_deleted(series)?;
		Ok(())
	}

	#[instrument(name = "catalog::series::list", level = "trace", skip(self, txn))]
	pub fn list_series(&self, txn: &mut Transaction<'_>) -> Result<Vec<Series>> {
		CatalogStore::list_series(txn)
	}

	#[instrument(name = "catalog::series::find_metadata", level = "trace", skip(self, txn))]
	pub fn find_series_metadata(
		&self,
		txn: &mut Transaction<'_>,
		id: SeriesId,
		partition: Partition,
	) -> Result<Option<SeriesPartitionMetadata>> {
		CatalogStore::find_series_metadata(txn, id, partition)
	}

	#[instrument(name = "catalog::series::update_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_series_metadata_txn(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Partition,
		metadata: SeriesPartitionMetadata,
	) -> Result<()> {
		CatalogStore::update_series_metadata_txn(txn, series_id, partition, metadata)
	}

	#[instrument(name = "catalog::series::update_metadata_rebased_txn", level = "debug", skip(self, txn, change))]
	pub fn update_series_metadata_rebased_txn(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Partition,
		metadata: SeriesPartitionMetadata,
		change: impl Fn(&mut SeriesPartitionMetadata) -> Result<()> + Send + Sync + 'static,
	) -> Result<()> {
		CatalogStore::update_series_metadata_rebased_txn(txn, series_id, partition, metadata, change)
	}
}
