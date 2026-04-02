// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSeriesChangeOperations,
	id::{NamespaceId, SeriesId},
	series::Series,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalSeriesChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackSeriesChangeOperations for AdminTransaction {
	fn track_series_created(&mut self, series: Series) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(series),
			op: Create,
		};
		self.changes.add_series_change(change);
		Ok(())
	}

	fn track_series_updated(&mut self, pre: Series, post: Series) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_series_change(change);
		Ok(())
	}

	fn track_series_deleted(&mut self, series: Series) -> Result<()> {
		let change = Change {
			pre: Some(series),
			post: None,
			op: Delete,
		};
		self.changes.add_series_change(change);
		Ok(())
	}
}

impl TransactionalSeriesChanges for AdminTransaction {
	fn find_series(&self, id: SeriesId) -> Option<&Series> {
		for change in self.changes.series.iter().rev() {
			if let Some(series) = &change.post
				&& series.id == id
			{
				return Some(series);
			}
			if let Some(series) = &change.pre
				&& series.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_series_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Series> {
		for change in self.changes.series.iter().rev() {
			if let Some(series) = &change.post
				&& series.namespace == namespace
				&& series.name == name
			{
				return Some(series);
			}
			if let Some(series) = &change.pre
				&& series.namespace == namespace
				&& series.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_series_deleted(&self, id: SeriesId) -> bool {
		self.changes
			.series
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id == id).unwrap_or(false))
	}

	fn is_series_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.series.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|s| s.namespace == namespace && s.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackSeriesChangeOperations for SubscriptionTransaction {
	fn track_series_created(&mut self, series: Series) -> Result<()> {
		self.inner.track_series_created(series)
	}

	fn track_series_updated(&mut self, pre: Series, post: Series) -> Result<()> {
		self.inner.track_series_updated(pre, post)
	}

	fn track_series_deleted(&mut self, series: Series) -> Result<()> {
		self.inner.track_series_deleted(series)
	}
}

impl TransactionalSeriesChanges for SubscriptionTransaction {
	fn find_series(&self, id: SeriesId) -> Option<&Series> {
		self.inner.find_series(id)
	}

	fn find_series_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Series> {
		self.inner.find_series_by_name(namespace, name)
	}

	fn is_series_deleted(&self, id: SeriesId) -> bool {
		self.inner.is_series_deleted(id)
	}

	fn is_series_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_series_deleted_by_name(namespace, name)
	}
}
