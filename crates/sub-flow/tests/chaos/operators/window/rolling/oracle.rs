// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_testing_chaos::operator::model::Model;
use reifydb_testing_sdk::chaos::rolling_model::{RollingModel, RollingRules};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

use crate::{framework::workload::WindowRow, operators::window::grid::Fold};

/// A rolling group trails its own newest coordinate; every leg of a batch is judged against the state the batch began
/// with.
pub struct Oracle {
	size_ms: u64,
	lateness_ms: u64,
	ledger: u64,
	model: RollingModel<i32, DateTime, i64>,
	fold: Fold,
}

fn at(ms: u64) -> DateTime {
	DateTime::from_epoch_millis(ms as i64).expect("rolling oracle coordinate")
}

fn span(ms: u64) -> Duration {
	Duration::from_milliseconds(ms as i64).expect("rolling oracle span")
}

fn model(size_ms: u64, lateness_ms: u64, retract_ms: u64) -> RollingModel<i32, DateTime, i64> {
	RollingModel::new(RollingRules {
		size: span(size_ms),
		lateness: span(lateness_ms),
		retract: span(retract_ms),
	})
}

impl Oracle {
	pub fn new(size_ms: u64, lateness_ms: u64) -> Self {
		Self {
			size_ms,
			lateness_ms,
			ledger: 0,
			model: model(size_ms, lateness_ms, lateness_ms),
			fold: Fold::Sum,
		}
	}

	/// Sum stays the default so the pinned rolling corpora keep the oracle they were recorded against.
	pub fn with_fold(mut self, fold: Fold) -> Self {
		self.fold = fold;
		self
	}

	/// A retraction is admitted back to immutable, an insert only back to lateness.
	pub fn with_immutable(mut self, immutable_ms: Option<u64>) -> Self {
		self.model = model(self.size_ms, self.lateness_ms, immutable_ms.unwrap_or(self.lateness_ms));
		self
	}

	fn folded(&self) -> Vec<Vec<Value>> {
		let mut out: Vec<Vec<Value>> = self
			.model
			.groups()
			.map(|(group, held)| {
				let values: Vec<i64> = held.into_iter().map(|(_, value)| *value).collect();
				vec![Value::Int4(*group), self.fold.apply(&values)]
			})
			.collect();
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
	}
}

impl Model<WindowRow> for Oracle {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, event: &WindowRow) -> bool {
		self.model.insert(event.group, event.number, at(event.coord_ms), event.value)
	}

	fn retract(&mut self, event: &WindowRow) {
		self.model.remove(event.group, event.number, at(event.coord_ms));
	}

	fn update(&mut self, pre: &WindowRow, post: &WindowRow) {
		self.model.update(
			(pre.group, pre.number, at(pre.coord_ms)),
			(post.group, post.number, at(post.coord_ms), post.value),
		);
	}

	fn step_complete(&mut self) {
		self.model.step();
	}

	fn advance_ledger(&mut self, at_ms: u64) {
		self.ledger = self.ledger.max(at_ms);
		if let Some(cutoff) = self.ledger.checked_sub(self.size_ms) {
			self.model.advance_cutoff(at(cutoff));
		}
	}

	fn live(&self) -> Vec<Vec<Value>> {
		self.folded()
	}

	fn all(&self) -> Vec<Vec<Value>> {
		// Must-publish and may-publish must be the same set, otherwise the two containment checks disagree.
		self.live()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		Vec::new()
	}
}
