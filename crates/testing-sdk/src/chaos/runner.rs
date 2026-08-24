// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_sdk::flow::operator::extern_c::binding::operator::ExternCOperator;
use reifydb_testing_chaos::{
	corpus::Corpus,
	operator::{
		compare::{ComparisonResult, Tolerances, compare},
		drive::drive,
		event::{ChaosBatch, ChaosEvent},
		model::Model,
		scenario::Scenario,
		view::{MaterializedView, RowKey},
	},
	seed::derive_seed,
};

use super::{
	bridge::{OracleClaim, OracleFn, ReplayModel, SamplerWorkload},
	context::ChaosContext,
	schema::ChaosSchema,
	strategy::ColumnRegistry,
};
use crate::harness::ExternCOperatorHarness;

#[derive(Debug)]
pub struct ChaosOutcome {
	pub context: ChaosContext,
	pub batches: Vec<ChaosBatch>,
	pub operator_table: MaterializedView,
	pub oracle_table: MaterializedView,
	pub comparison: ComparisonResult,

	pub corpus: Corpus,

	pub divergence: Option<String>,
}

impl ChaosOutcome {
	pub fn is_match(&self) -> bool {
		self.comparison.is_match() && self.comparison.is_coherent() && self.divergence.is_none()
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
		if let Some(report) = &self.divergence {
			header.push(format!("  the drive loop stopped early: {report}"));
		}
		let report = self.comparison.format_failure(&header, 5);
		panic!("\n{report}");
	}
}

pub struct RunnableChaos<T: ExternCOperator> {
	pub context: ChaosContext,
	pub scenario: Scenario,
	pub schema: Arc<ChaosSchema>,
	pub registry: Arc<ColumnRegistry>,
	pub tolerances: Tolerances,
	pub oracle: OracleFn,
	pub harness: ExternCOperatorHarness<T>,
}

impl<T: ExternCOperator> RunnableChaos<T> {
	pub fn run(mut self) -> ChaosOutcome {
		let scenario = self.scenario;
		let workload = SamplerWorkload::new(self.schema.clone(), self.registry.clone());
		let mut model = ReplayModel::claiming(OracleClaim {
			context: self.context.clone(),
			oracle: self.oracle.clone(),
			key_columns: self.schema.output_key_columns.clone(),
			tolerances: self.tolerances.clone(),
		});

		let seed = self.context.seed;
		let driven = drive(derive_seed(seed, 1), scenario, &mut self.harness, &workload, &mut model);

		let oracle_table = model.after_drain().map(|claim| claim.view).unwrap_or_else(MaterializedView::empty);

		let operator_table = driven.view.rekey(&RowKey::columns(self.schema.output_key_columns.clone()));
		let comparison = compare(&operator_table, &oracle_table, &self.tolerances);

		ChaosOutcome {
			context: self.context,
			batches: model.into_log(),
			operator_table,
			oracle_table,
			comparison,
			corpus: driven.corpus,
			divergence: driven.divergence,
		}
	}
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
			divergence: None,
			corpus: Corpus::new(0, 0),
		};
		assert!(outcome.is_match());
		outcome.assert_matches();
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
			divergence: None,
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
