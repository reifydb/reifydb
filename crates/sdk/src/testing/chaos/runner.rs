// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_testing_chaos::{
	corpus::Corpus,
	operator::{
		compare::{ComparisonResult, Tolerances, compare},
		drive::drive,
		view::MaterializedView,
	},
	seed::derive_seed,
};
use reifydb_value::value::datetime::DateTime;

use super::{
	bridge::{ReplayModel, SamplerWorkload},
	config::ChaosConfig,
	context::ChaosContext,
	event::{ChaosBatch, ChaosEvent},
	materialize::materialize_history,
	schema::ChaosSchema,
	strategy::ColumnRegistry,
};
use crate::{operator::FFIOperator, testing::harness::FFIOperatorHarness};

pub type OracleFn = Arc<dyn Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedView + Send + Sync>;

#[derive(Debug)]
pub struct ChaosOutcome {
	pub context: ChaosContext,
	pub batches: Vec<ChaosBatch>,
	pub operator_table: MaterializedView,
	pub oracle_table: MaterializedView,
	pub comparison: ComparisonResult,

	pub corpus: Corpus,

	pub incoherent: Vec<String>,
}

impl ChaosOutcome {
	pub fn is_match(&self) -> bool {
		self.comparison.is_match() && self.incoherent.is_empty()
	}

	#[track_caller]
	pub fn assert_pinned(&self, expected: u64) {
		self.corpus.assert_pinned(expected);
	}

	pub fn fingerprint(&self) -> u64 {
		self.corpus.fingerprint()
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
		let scenario = self.config.to_scenario(0, 0);
		let workload = SamplerWorkload::new(self.schema.clone(), self.registry.clone());
		let mut model = ReplayModel::new();

		let seed = self.context.seed;
		let corpus = drive(derive_seed(seed, 1), scenario, &mut self.harness, &workload, &mut model)
			.unwrap_or_else(|report| panic!("chaos divergence\n  seed: {seed}\n{report}"));

		let batches = model.into_log();

		if let Some(at) = highest_event_time(&batches) {
			self.harness.advance_watermark(at).expect("watermark drain failed during chaos run");
		}

		let operator_history: Vec<_> =
			(0..self.harness.history_len()).map(|i| self.harness[i].clone()).collect();
		let operator_table = materialize_history(&operator_history, &self.schema.output_key_columns);
		let oracle_table = (self.oracle)(&self.context, &batches);
		let comparison = compare(&operator_table, &oracle_table, &self.tolerances);

		ChaosOutcome {
			context: self.context,
			batches,
			operator_table,
			oracle_table,
			comparison,
			incoherent: Vec::new(),
			corpus,
		}
	}
}

fn highest_event_time(batches: &[ChaosBatch]) -> Option<DateTime> {
	batches.iter().flat_map(|batch| batch.iter()).map(|event| event.row().encoded.time()).max()
}

#[cfg(test)]
mod tests {
	use reifydb_testing_chaos::operator::view::{MaterializedRow, OutputKey};
	use reifydb_value::value::Value;

	use super::*;

	#[test]
	fn outcome_match_does_not_panic() {
		let outcome = ChaosOutcome {
			context: ChaosContext::new(42),
			batches: vec![],
			operator_table: MaterializedView::empty(),
			oracle_table: MaterializedView::empty(),
			comparison: ComparisonResult::default(),
			incoherent: Vec::new(),
			corpus: Corpus::new(0, 0),
		};
		assert!(outcome.is_match());
		outcome.assert_matches(); // should not panic
	}

	#[test]
	#[should_panic(expected = "chaos divergence")]
	fn outcome_mismatch_panics_with_seed() {
		let mut op = MaterializedView::empty();
		op.insert(
			OutputKey::new(vec![Value::uint8(1u64)]),
			MaterializedRow::from_pairs(vec![("v".to_string(), Value::float8(2.0_f64))]),
		);
		let oracle = MaterializedView::empty();
		let outcome = ChaosOutcome {
			context: ChaosContext::new(12345),
			batches: vec![],
			operator_table: op.clone(),
			oracle_table: oracle.clone(),
			comparison: compare(&op, &oracle, &Tolerances::new()),
			incoherent: Vec::new(),
			corpus: Corpus::new(0, 0),
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
