// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, SeriesId},
		series::Series,
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionSeries};

impl MaterializedCatalog {
	/// Find a series by ID at a specific version
	pub fn find_series_at(&self, series: SeriesId, version: CommitVersion) -> Option<Series> {
		self.series.get(&series).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a series by name in a namespace at a specific version
	pub fn find_series_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Series> {
		self.series_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let series_id = *entry.value();
			self.find_series_at(series_id, version)
		})
	}

	/// Find a series by ID (returns latest version)
	pub fn find_series(&self, series: SeriesId) -> Option<Series> {
		self.series.get(&series).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Find a series by name in a namespace (returns latest version)
	pub fn find_series_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Series> {
		self.series_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let series_id = *entry.value();
			self.find_series(series_id)
		})
	}

	/// List the latest version of all series.
	pub fn list_series(&self) -> Vec<Series> {
		self.series.iter().filter_map(|entry| entry.value().get_latest()).collect()
	}

	pub fn set_series(&self, id: SeriesId, version: CommitVersion, series: Option<Series>) {
		if let Some(entry) = self.series.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.series_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.series.get_or_insert_with(id, MultiVersionSeries::new);
		if let Some(new) = series {
			self.series_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
