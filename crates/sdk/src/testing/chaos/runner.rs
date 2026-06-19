// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	sync::Arc,
};

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
}

impl ChaosOutcome {
	pub fn is_match(&self) -> bool {
		self.comparison.is_match()
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
		let header = vec![
			format!("chaos divergence:"),
			format!("  seed: {}", self.context.seed),
			format!("  batches: {}", self.batches.len()),
			format!("  ops: {}", self.ops_count()),
		];
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
		}
	}
}

fn derive_seed(master: u64, salt: u64) -> u64 {
	let mut h = DefaultHasher::new();
	Hash::hash(&master, &mut h);
	Hash::hash(&salt, &mut h);
	h.finish()
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
