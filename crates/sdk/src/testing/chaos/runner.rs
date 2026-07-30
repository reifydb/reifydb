// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_testing_chaos::{
	corpus::{Corpus, mix},
	operator::{
		compare::{ComparisonResult, Tolerances, compare},
		table::MaterializedTable,
		view::View,
	},
	seed::derive_seed,
};
use reifydb_value::value::datetime::DateTime;

use super::{
	batcher::Batcher,
	config::ChaosConfig,
	context::ChaosContext,
	event::{ChaosBatch, ChaosEvent},
	generator::Generator,
	materialize::materialize_history,
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
	/// Fingerprint of the operation sequence this seed actually produced.
	///
	/// Guest suites pin hardcoded seeds to demonstrate a defect class, and a seed only means something
	/// in terms of the generator that consumes it. Widen a sampler range or add a mutation branch and
	/// every one of those pins silently points at a different sequence - one that may no longer contain
	/// the defect it names - while staying green. Recording the fingerprint turns that into a loud
	/// failure at the point of change.
	pub corpus: Corpus,
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

	/// Asserts this seed still produces the sequence a pinned test was written against.
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
		let corpus = fingerprint_corpus(self.context.seed, &batches);
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
			corpus,
		}
	}
}

/// Folds the generated event log into a fingerprint.
///
/// Mixes the operation KIND and the row identity, in sequence order, so a reordering registers as a
/// different corpus. Row VALUES are deliberately excluded: several samplers draw floats, and a
/// fingerprint that moved when a float's last bit changed would fire on noise rather than on a real
/// generator change.
fn fingerprint_corpus(seed: u64, batches: &[ChaosBatch]) -> Corpus {
	let mut fingerprint = mix(0, seed);
	let mut steps = 0usize;
	for batch in batches {
		fingerprint = mix(fingerprint, batch.len() as u64);
		for event in batch.iter() {
			let kind = match event {
				ChaosEvent::Insert { .. } => 1,
				ChaosEvent::Update { .. } => 2,
				ChaosEvent::Remove { .. } => 3,
			};
			fingerprint = mix(mix(fingerprint, kind), event.row_number().0);
			steps += 1;
		}
	}
	Corpus::new(fingerprint, steps)
}

fn highest_event_time(batches: &[ChaosBatch]) -> Option<DateTime> {
	batches.iter().flat_map(|batch| batch.iter()).map(|event| event.row().encoded.time()).max()
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use reifydb_testing_chaos::operator::table::{MaterializedRow, OutputKey};

	use super::*;

	#[test]
	fn outcome_match_does_not_panic() {
		let outcome = ChaosOutcome {
			context: ChaosContext::new(42),
			batches: vec![],
			operator_table: MaterializedTable::empty(),
			oracle_table: MaterializedTable::empty(),
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
