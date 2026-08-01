// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Drives seeded Insert/Update/Remove diffs through an FFI operator and an
//! author-supplied naive oracle, then asserts the two materialized tables
//! agree. Entrypoint: [`ChaosHarness::builder`].

use std::{
	error::Error,
	fmt::{self, Display, Formatter},
	marker::PhantomData,
	mem,
	ops::Range,
	sync::Arc,
};

use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId};
use reifydb_value::value::Value;

pub mod accumulator_oracle;
pub mod bridge;
pub mod context;
pub mod materialize;
pub mod runner;
pub mod schema;
pub mod strategy;

use bridge::OracleFn;
use context::ChaosContext;
use reifydb_sdk::operator::FFIOperator;
use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	event::ChaosBatch,
	scenario::{Scenario, SupportedOps},
	view::MaterializedView,
};
use runner::RunnableChaos;
use schema::{ChaosSchema, KeyStrategy};
use strategy::{ColumnRegistry, ColumnSampler, RowContent, samplers};

use crate::harness::FFIOperatorHarness;

#[derive(Debug)]
pub enum ChaosError {
	/// Update or Remove without Insert: the driver can never populate live rows.
	UnreachableSupportedOps,

	MissingField(&'static str),

	/// `output_key` names a column absent from `output_shape`.
	OutputKeyColumnMissing(String),

	InputColumnsMissingSampler(Vec<String>),

	/// The inner FFI operator harness rejected the config or failed to initialize.
	HarnessBuild(String),
}

impl Display for ChaosError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ChaosError::UnreachableSupportedOps => write!(
				f,
				"SupportedOps configuration is unreachable: enabling Update or Remove without Insert leaves the driver with no way to populate live rows"
			),
			ChaosError::MissingField(name) => write!(f, "missing required builder field: {name}"),
			ChaosError::OutputKeyColumnMissing(col) => {
				write!(f, "output_key column '{col}' is not present in output_shape")
			}
			ChaosError::InputColumnsMissingSampler(cols) => {
				write!(f, "input columns without samplers: {cols:?}")
			}
			ChaosError::HarnessBuild(msg) => write!(f, "operator harness build failed: {msg}"),
		}
	}
}

impl Error for ChaosError {}

pub type ChaosResult<T> = Result<T, ChaosError>;

const DEFAULT_STEPS: u32 = 200;

/// Namespace only; the active object is the [`RunnableChaos`] that `build()` returns.
pub struct ChaosHarness<T: FFIOperator> {
	_phantom: PhantomData<T>,
}

impl<T: FFIOperator> ChaosHarness<T> {
	pub fn builder() -> ChaosHarnessBuilder<T> {
		ChaosHarnessBuilder::new()
	}
}

/// Required: input/output shape, key strategy, output key, one sampler per input
/// column, and an oracle. Everything else has a default.
pub struct ChaosHarnessBuilder<T: FFIOperator> {
	seed: u64,
	scenario: Scenario,
	supported_ops: SupportedOps,
	node_id: FlowNodeId,
	version: CommitVersion,
	operator_config: Vec<(String, Value)>,
	input_shape: Option<RowShape>,
	output_shape: Option<RowShape>,
	key_strategy: Option<KeyStrategy>,
	output_key_columns: Vec<String>,
	time_column: Option<String>,
	registry: ColumnRegistry,
	tolerances: Tolerances,
	oracle: Option<OracleFn>,
	_phantom: PhantomData<T>,
}

impl<T: FFIOperator> Default for ChaosHarnessBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: FFIOperator> ChaosHarnessBuilder<T> {
	pub fn new() -> Self {
		Self {
			seed: 0,
			scenario: Scenario::mixed(DEFAULT_STEPS),
			supported_ops: SupportedOps::default(),
			node_id: FlowNodeId(1),
			version: CommitVersion(1),
			operator_config: Vec::new(),
			input_shape: None,
			output_shape: None,
			key_strategy: None,
			output_key_columns: Vec::new(),
			time_column: None,
			registry: ColumnRegistry::new(),
			tolerances: Tolerances::new(),
			oracle: None,
			_phantom: PhantomData,
		}
	}

	pub fn seed(mut self, seed: u64) -> Self {
		self.seed = seed;
		self
	}

	pub fn with_scenario(mut self, scenario: Scenario) -> Self {
		self.scenario = scenario;
		self
	}

	pub fn with_supported_ops(mut self, ops: SupportedOps) -> Self {
		self.supported_ops = ops;
		self.scenario = self.scenario.with_ops(ops);
		self
	}

	pub fn with_node_id(mut self, node_id: FlowNodeId) -> Self {
		self.node_id = node_id;
		self
	}

	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	/// Config handed to `T::new`; mirrors
	/// [`crate::harness::FFIOperatorHarnessBuilder::with_config`].
	pub fn with_config<I, K>(mut self, config: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		self.operator_config = config.into_iter().map(|(k, v)| (k.into(), v)).collect();
		self
	}

	pub fn with_input_shape(mut self, shape: RowShape) -> Self {
		self.input_shape = Some(shape);
		self
	}

	pub fn with_time_column(mut self, column: impl Into<String>) -> Self {
		self.time_column = Some(column.into());
		self
	}

	pub fn with_output_shape(mut self, shape: RowShape) -> Self {
		self.output_shape = Some(shape);
		self
	}

	pub fn with_key_strategy(mut self, key_strategy: KeyStrategy) -> Self {
		self.key_strategy = Some(key_strategy);
		self
	}

	pub fn with_output_key<I, S>(mut self, columns: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		self.output_key_columns = columns.into_iter().map(Into::into).collect();
		self
	}

	/// Samplers come from [`samplers`], or hand-roll an
	/// `Arc<dyn Fn(&mut StdRng) -> Value + Send + Sync>`.
	pub fn with_column(mut self, name: impl Into<String>, sampler: ColumnSampler) -> Self {
		self.registry.register(name, sampler);
		self
	}

	/// Runs after per-column sampling, so it can derive or override sampled values.
	pub fn with_row_constraints(mut self, f: impl Fn(&mut RowContent) + Send + Sync + 'static) -> Self {
		self.registry.set_constraint(Arc::new(f));
		self
	}

	pub fn with_tolerance(mut self, column: impl Into<String>, tol: f64) -> Self {
		self.tolerances = mem::take(&mut self.tolerances).with(column, tol);
		self
	}

	/// Required. The oracle sees one `ChaosBatch` per `Change` the operator's
	/// `apply()` saw; windowed oracles snapshot at the end of each batch.
	pub fn with_oracle<F>(mut self, f: F) -> Self
	where
		F: Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedView + Send + Sync + 'static,
	{
		self.oracle = Some(Arc::new(f));
		self
	}

	/// Validation rules live in [`ChaosSchema::validate`] and
	/// [`ColumnRegistry::validate`]; this only maps their errors onto `ChaosError`.
	pub fn build(self) -> ChaosResult<RunnableChaos<T>> {
		validate_supported_ops(&self.supported_ops)?;
		let input_shape = self.input_shape.ok_or(ChaosError::MissingField("input_shape"))?;
		let output_shape = self.output_shape.ok_or(ChaosError::MissingField("output_shape"))?;
		let key_strategy = self.key_strategy.ok_or(ChaosError::MissingField("key_strategy"))?;
		if self.output_key_columns.is_empty() {
			return Err(ChaosError::MissingField("output_key"));
		}
		let oracle = self.oracle.ok_or(ChaosError::MissingField("oracle"))?;

		let schema = ChaosSchema {
			input_shape,
			output_shape,
			key_strategy,
			output_key_columns: self.output_key_columns,
			time_column: self.time_column,
		};
		schema.validate().map_err(ChaosError::OutputKeyColumnMissing)?;
		self.registry.validate(&schema.input_shape).map_err(ChaosError::InputColumnsMissingSampler)?;
		let schema = Arc::new(schema);

		let context = ChaosContext::new(self.seed);

		let mut builder = FFIOperatorHarness::<T>::builder()
			.with_node_id(self.node_id)
			.with_version(self.version)
			.with_clock(context.clock.clone());
		for (k, v) in self.operator_config {
			builder = builder.add_config(k, v);
		}
		let harness = builder.build().map_err(|e| ChaosError::HarnessBuild(format!("{e:?}")))?;

		Ok(RunnableChaos {
			context,
			scenario: self.scenario,
			schema,
			registry: Arc::new(self.registry),
			tolerances: self.tolerances,
			oracle,
			harness,
		})
	}
}

fn validate_supported_ops(ops: &SupportedOps) -> ChaosResult<()> {
	if ops.is_reachable() {
		Ok(())
	} else {
		Err(ChaosError::UnreachableSupportedOps)
	}
}

/// Lets authors write `.with_column("k", 1u64..1000)` instead of
/// `.with_column("k", samplers::u64_range(1..1000))`.
pub trait IntoColumnSampler {
	fn into_sampler(self) -> ColumnSampler;
}

impl IntoColumnSampler for ColumnSampler {
	fn into_sampler(self) -> ColumnSampler {
		self
	}
}

impl IntoColumnSampler for Range<u64> {
	fn into_sampler(self) -> ColumnSampler {
		samplers::u64_range(self)
	}
}

impl IntoColumnSampler for Range<u32> {
	fn into_sampler(self) -> ColumnSampler {
		samplers::u32_range(self)
	}
}

impl IntoColumnSampler for Range<i64> {
	fn into_sampler(self) -> ColumnSampler {
		samplers::i64_range(self)
	}
}

impl IntoColumnSampler for Range<f64> {
	fn into_sampler(self) -> ColumnSampler {
		samplers::f64_range(self)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_sdk::{
		config::Config,
		error::Result,
		operator::{
			FFIOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
			context::ffi::FFIOperatorContext,
		},
	};
	use reifydb_testing_chaos::operator::scenario::BatchSize;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	/// Monomorphizes the chaos builder for validation tests, which run before
	/// the operator is ever invoked.
	struct NoOpOperator;

	impl OperatorMetadata for NoOpOperator {
		const NAME: &'static str = "noop";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "no-op operator for chaos builder tests";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl FFIOperator for NoOpOperator {
		fn new(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, _ctx: &mut FFIOperatorContext, _input: BorrowedChange<'_>) -> Result<()> {
			Ok(())
		}
	}

	fn shape(fields: &[(&str, ValueType)]) -> RowShape {
		RowShape::new(fields.iter().map(|(n, t)| RowShapeField::unconstrained(*n, t.clone())).collect())
	}

	#[test]
	fn types_compile() {
		let _ = Scenario::mixed(DEFAULT_STEPS);
		let _ = SupportedOps::default();
		let _ = SupportedOps::insert_only();
		let _ = SupportedOps::no_remove();
		let _ = SupportedOps::no_update();
		let _ = BatchSize::Constant(1);
		let _ = BatchSize::Uniform {
			min: 1,
			max: 10,
		};
		let _ = BatchSize::Geometric {
			p: 0.4,
			max: 8,
		};
		let _ = MaterializedView::empty();
	}

	#[test]
	fn the_builder_default_is_the_mixed_corpus_it_replaced() {
		// Every suite that skips with_scenario runs this corpus; drift here re-points all of
		// them while they all still pass.
		let builder = ChaosHarness::<NoOpOperator>::builder();
		assert_eq!(builder.scenario.steps, 200);
		assert_eq!(builder.scenario.max_live, Some(50));
		assert_eq!(builder.scenario.duplicate_update_burst, 0.3);
		assert_eq!(builder.scenario.update_as_remove_insert, 0.1);
		assert_eq!(
			builder.scenario.batch,
			BatchSize::Geometric {
				p: 0.4,
				max: 8,
			}
		);
		assert!(builder.scenario.remove_pct > 0 && builder.scenario.update_pct > 0);
	}

	#[test]
	fn supported_ops_reshapes_the_scenario_mix_in_place() {
		// If the preset stopped writing through to the mix, a suite asking to isolate inserts
		// would still generate removes.
		let builder = ChaosHarness::<NoOpOperator>::builder().with_supported_ops(SupportedOps::insert_only());
		assert_eq!(builder.scenario.remove_pct, 0);
		assert_eq!(builder.scenario.update_pct, 0);
	}

	#[test]
	fn unreachable_supported_ops_rejected() {
		let bad = SupportedOps {
			insert: false,
			update: true,
			remove: false,
		};
		assert!(matches!(validate_supported_ops(&bad), Err(ChaosError::UnreachableSupportedOps)));

		let also_bad = SupportedOps {
			insert: false,
			update: false,
			remove: true,
		};
		assert!(matches!(validate_supported_ops(&also_bad), Err(ChaosError::UnreachableSupportedOps)));
	}

	#[test]
	fn reachable_supported_ops_accepted() {
		assert!(validate_supported_ops(&SupportedOps::all()).is_ok());
		assert!(validate_supported_ops(&SupportedOps::insert_only()).is_ok());
		assert!(validate_supported_ops(&SupportedOps::no_remove()).is_ok());
		assert!(validate_supported_ops(&SupportedOps::no_update()).is_ok());
	}

	#[test]
	fn empty_supported_ops_is_unreachable() {
		// All-disabled is useless but not unreachable; the caller owns num_ops > 0.
		let none = SupportedOps {
			insert: false,
			update: false,
			remove: false,
		};
		assert!(validate_supported_ops(&none).is_ok());
	}

	fn well_formed_builder() -> ChaosHarnessBuilder<NoOpOperator> {
		// The minimum settings that reach validation; each test then breaks one field.
		ChaosHarness::<NoOpOperator>::builder()
			.with_input_shape(shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8)]))
			.with_output_shape(shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8)]))
			.with_key_strategy(KeyStrategy::Sequential)
			.with_output_key(["k"])
			.with_column("k", samplers::u64_range(1..1000))
			.with_column("v", samplers::f64_range(0.0..1.0))
			.with_oracle(|_, _| MaterializedView::empty())
	}

	#[test]
	fn build_accepts_well_formed_builder() {
		assert!(well_formed_builder().build().is_ok(), "expected well-formed builder to succeed");
	}

	fn expect_build_err(result: ChaosResult<RunnableChaos<NoOpOperator>>, label: &str) -> ChaosError {
		// RunnableChaos is not Debug, so Result::expect_err is unavailable.
		match result {
			Ok(_) => panic!("expected error from build(): {label}"),
			Err(e) => e,
		}
	}

	#[test]
	fn build_rejects_typoed_output_key() {
		// If build() bypassed schema validation, the schema-level test would still pass while
		// typos slipped through here.
		let err =
			expect_build_err(well_formed_builder().with_output_key(["typo"]).build(), "typo'd output_key");
		match err {
			ChaosError::OutputKeyColumnMissing(col) => assert_eq!(col, "typo"),
			other => panic!("expected OutputKeyColumnMissing(\"typo\"), got {other:?}"),
		}
	}

	#[test]
	fn build_rejects_input_columns_without_samplers() {
		// The same wiring assertion for the sampler registry.
		let result = ChaosHarness::<NoOpOperator>::builder()
			.with_input_shape(shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8), ("missing", ValueType::Int8)]))
			.with_output_shape(shape(&[("k", ValueType::Uint8)]))
			.with_key_strategy(KeyStrategy::Sequential)
			.with_output_key(["k"])
			.with_column("k", samplers::u64_range(1..1000))
			.with_column("v", samplers::f64_range(0.0..1.0))
			// "missing" intentionally not registered.
			.with_oracle(|_, _| MaterializedView::empty())
			.build();
		match expect_build_err(result, "missing sampler") {
			ChaosError::InputColumnsMissingSampler(cols) => {
				assert_eq!(cols, vec!["missing".to_string()]);
			}
			other => panic!("expected InputColumnsMissingSampler, got {other:?}"),
		}
	}

	#[test]
	fn build_rejects_missing_required_fields() {
		let err = expect_build_err(ChaosHarness::<NoOpOperator>::builder().build(), "no input_shape");
		assert!(matches!(err, ChaosError::MissingField("input_shape")), "{err:?}");

		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.build(),
			"no output_shape",
		);
		assert!(matches!(err, ChaosError::MissingField("output_shape")), "{err:?}");

		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.with_output_shape(shape(&[("k", ValueType::Uint8)]))
				.build(),
			"no key_strategy",
		);
		assert!(matches!(err, ChaosError::MissingField("key_strategy")), "{err:?}");

		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.with_output_shape(shape(&[("k", ValueType::Uint8)]))
				.with_key_strategy(KeyStrategy::Sequential)
				.build(),
			"no output_key",
		);
		assert!(matches!(err, ChaosError::MissingField("output_key")), "{err:?}");

		// Every other required field must be present or an earlier check shadows the oracle error.
		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.with_output_shape(shape(&[("k", ValueType::Uint8)]))
				.with_key_strategy(KeyStrategy::Sequential)
				.with_output_key(["k"])
				.with_column("k", samplers::u64_range(1..1000))
				.build(),
			"no oracle",
		);
		assert!(matches!(err, ChaosError::MissingField("oracle")), "{err:?}");
	}
}
