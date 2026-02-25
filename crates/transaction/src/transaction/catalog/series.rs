// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSeriesChangeOperations,
	id::{NamespaceId, SeriesId},
	series::SeriesDef,
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalSeriesChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackSeriesChangeOperations for AdminTransaction {
	fn track_series_def_created(&mut self, series: SeriesDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(series),
			op: Create,
		};
		self.changes.add_series_def_change(change);
		Ok(())
	}

	fn track_series_def_updated(&mut self, pre: SeriesDef, post: SeriesDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_series_def_change(change);
		Ok(())
	}

	fn track_series_def_deleted(&mut self, series: SeriesDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(series),
			post: None,
			op: Delete,
		};
		self.changes.add_series_def_change(change);
		Ok(())
	}
}

impl TransactionalSeriesChanges for AdminTransaction {
	fn find_series(&self, id: SeriesId) -> Option<&SeriesDef> {
		for change in self.changes.series_def.iter().rev() {
			if let Some(series) = &change.post {
				if series.id == id {
					return Some(series);
				}
			}
			if let Some(series) = &change.pre {
				if series.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_series_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SeriesDef> {
		for change in self.changes.series_def.iter().rev() {
			if let Some(series) = &change.post {
				if series.namespace == namespace && series.name == name {
					return Some(series);
				}
			}
			if let Some(series) = &change.pre {
				if series.namespace == namespace && series.name == name && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_series_deleted(&self, id: SeriesId) -> bool {
		self.changes
			.series_def
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id == id).unwrap_or(false))
	}

	fn is_series_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.series_def.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|s| s.namespace == namespace && s.name == name)
					.unwrap_or(false)
		})
	}
}
