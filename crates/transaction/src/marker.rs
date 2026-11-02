// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::id::ViewId,
	marker::{MARKER_T0_CAP, Marker, MarkerEntry, MarkerSink},
};
use reifydb_runtime::context::clock::{Clock, Instant};

use crate::error::TransactionError;

pub(crate) struct PendingMarkers {
	sink: Arc<dyn MarkerSink>,
	clock: Clock,
	writer: Option<Arc<[Instant]>>,
	by_view: BTreeMap<ViewId, Vec<Marker>>,
}

impl PendingMarkers {
	pub(crate) fn new(sink: Arc<dyn MarkerSink>, clock: Clock) -> Self {
		Self {
			sink,
			clock,
			writer: None,
			by_view: BTreeMap::new(),
		}
	}

	pub(crate) fn attach_writer(&mut self, t0s: Arc<[Instant]>) -> Result<(), TransactionError> {
		if t0s.len() > MARKER_T0_CAP {
			return Err(TransactionError::MarkerTooLarge {
				count: t0s.len(),
				limit: MARKER_T0_CAP,
			});
		}
		if self.writer.is_some() {
			return Err(TransactionError::MarkerAlreadyAttached);
		}
		self.writer = Some(t0s);
		Ok(())
	}

	pub(crate) fn attach_view(&mut self, view: ViewId, markers: Vec<Marker>) -> Result<(), TransactionError> {
		if self.by_view.contains_key(&view) {
			return Err(TransactionError::ViewMarkersAlreadyAttached {
				view,
			});
		}
		self.by_view.insert(view, markers);
		Ok(())
	}

	pub(crate) fn insert_into_sink(self, version: CommitVersion) {
		if version == CommitVersion(0) {
			return;
		}
		self.sink.insert(
			version,
			MarkerEntry {
				committed_at: self.clock.instant(),
				writer: self.writer.map(|t0s| Marker {
					id: version,
					t0s,
				}),
				by_view: self.by_view,
			},
		);
	}
}
