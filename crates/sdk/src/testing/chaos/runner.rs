// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_testing_chaos::{operator::view::View, seed::derive_seed};
use reifydb_value::value::datetime::DateTime;

use super::{
	batcher::Batcher,
	config::ChaosConfig,
	context::ChaosContext,
	event::{ChaosBatch, ChaosEvent},
	generator::Generator,
	materialize::materialize_history,
	oracle::MaterializedTable,
	report::{ComparisonResult, Tolerances, compare},
	schema::ChaosSchema,
	strategy::ColumnRegistry,
};
use crate::{operator::FFIOperator, testing::harness::FFIOperatorHarness};

pub type OracleFn = Arc<dyn Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedTable + Send + Sync>;

#[derive(Debug)]
pub struct ChaosOutcome {
	pub context: ChaosContext,
	pub batches: Vec<ChaosBatch>,
	pub operator_table: MaterializedTable,
	pub oracle_table: MaterializedTable,
	pub comparison: ComparisonResult,
	/// Ways the operator's diff stream could not be folded, independent of whether its VALUES agree
	/// with the oracle.
	///
	/// The table comparison keys on the operator's own output columns, so it cannot see this class of
	/// defect at all: a row inserted twice, an update whose pre-image was never published, a remove of
	/// something absent. Each of those leaves a downstream consumer with a view the operator never
	/// intended, and each can happen while every value still matches.
	pub incoherent: Vec<String>,
}

impl ChaosOutcome {
	pub fn is_match(&self) -> bool {
		self.comparison.is_match() && self.incoherent.is_empty()
	}

	pub fn ops_count(&self) -> usize {
		self.batches.iter().map(|b| b.len()).sum()
	}

	pub fn events(&self) -> impl Iterator<Item = &ChaosEvent> {
		self.batches.iter().flat_map(|b| b.iter())
	}

	pub fn assert_matches(&self) {
		if self.is_match() {
			return;
		}
		let mut header = vec![
			format!("chaos divergence:"),
			format!("  seed: {}", self.context.seed),
			format!("  batches: {}", self.batches.len()),
			format!("  ops: {}", self.ops_count()),
		];
		for problem in self.incoherent.iter().take(5) {
			header.push(format!("  unfoldable diff stream: {problem}"));
		}
		let report = self.comparison.format_failure(&header, 5);
		panic!("\n{report}");
	}
}

pub struct RunnableChaos<T: FFIOperator> {
	pub context: ChaosContext,
	pub config: ChaosConfig,
	pub schema: Arc<ChaosSchema>,
	pub registry: Arc<ColumnRegistry>,
	pub tolerances: Tolerances,
	pub oracle: OracleFn,
	pub harness: FFIOperatorHarness<T>,
}

impl<T: FFIOperator> RunnableChaos<T> {
	pub fn run(mut self) -> ChaosOutcome {
		let mut generator = Generator::new(
			self.schema.clone(),
			self.registry.clone(),
			self.config,
			derive_seed(self.context.seed, 1),
		);
		let mut batcher = Batcher::new(self.config.batch_size, derive_seed(self.context.seed, 2));

		while let Some(change) = batcher.next_change(&mut generator) {
			self.harness.apply(change).expect("operator apply failed during chaos run");
		}

		let batches = batcher.take_logical_log();
		if let Some(at) = highest_event_time(&batches) {
			self.harness.advance_watermark(at).expect("watermark drain failed during chaos run");
		}
		let operator_history: Vec<_> =
			(0..self.harness.history_len()).map(|i| self.harness[i].clone()).collect();
		// Fold the same history a second time, keyed by row number rather than by output column, purely
		// to harvest the coherence findings the value comparison structurally cannot produce.
		let mut view = View::new();
		for change in &operator_history {
			view.apply(change);
		}
		let operator_table = materialize_history(&operator_history, &self.schema.output_key_columns);
		let oracle_table = (self.oracle)(&self.context, &batches);
		let comparison = compare(&operator_table, &oracle_table, &self.tolerances);

		ChaosOutcome {
			context: self.context,
			batches,
			operator_table,
			oracle_table,
			comparison,
			incoherent: view.incoherent,
		}
	}
}

fn highest_event_time(batches: &[ChaosBatch]) -> Option<DateTime> {
	batches.iter().flat_map(|batch| batch.iter()).map(|event| event.row().encoded.time()).max()
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::{
		super::oracle::{MaterializedRow, OutputKey},
		*,
	};

	#[test]
	fn outcome_match_does_not_panic() {
		let outcome = ChaosOutcome {
			context: ChaosContext::new(42),
			batches: vec![],
			operator_table: MaterializedTable::empty(),
			oracle_table: MaterializedTable::empty(),
			comparison: ComparisonResult::default(),
			incoherent: Vec::new(),
		};
		assert!(outcome.is_match());
		outcome.assert_matches(); // should not panic
	}

	#[test]
	#[should_panic(expected = "chaos divergence")]
	fn outcome_mismatch_panics_with_seed() {
		let mut op = MaterializedTable::empty();
		op.insert(
			OutputKey::new(vec![Value::uint8(1u64)]),
			MaterializedRow::from_pairs(vec![("v".to_string(), Value::float8(2.0_f64))]),
		);
		let oracle = MaterializedTable::empty();
		let outcome = ChaosOutcome {
			context: ChaosContext::new(12345),
			batches: vec![],
			operator_table: op.clone(),
			oracle_table: oracle.clone(),
			comparison: compare(&op, &oracle, &Tolerances::new()),
			incoherent: Vec::new(),
		};
		assert!(!outcome.is_match());
		outcome.assert_matches();
	}

	#[test]
	fn derive_seed_is_deterministic_and_decorrelated() {
		assert_eq!(derive_seed(1, 1), derive_seed(1, 1));
		assert_ne!(derive_seed(1, 1), derive_seed(1, 2));
		assert_ne!(derive_seed(1, 1), derive_seed(2, 1));
	}
}
