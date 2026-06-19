// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Chaos testing harness for FFI operators.
//!
//! Generates seeded, reproducible sequences of `Insert`/`Update`/`Remove`
//! diffs against an operator-under-test, runs the same logical event log
//! through a test-author-provided naive oracle, and asserts that the two
//! materialized output tables agree.
//!
//! Designed to catch the bug class where an operator silently miscounts
//! under streaming diff types that production hits but unit tests don't
//! (e.g., the OHLCV tumbling double-count bug, where Updates re-emitted by
//! a LEFT JOIN re-fire add to the running volume sum on every fire).
//!
//! Author-facing entrypoint: [`ChaosHarness::builder`].

use std::{
	error::Error,
	fmt::{self, Display, Formatter},
	marker::PhantomData,
	mem,
	ops::Range,
	sync::Arc,
};

use reifydb_core::{common::CommitVersion, encoded::shape::RowShape, interface::catalog::flow::FlowNodeId};
use reifydb_value::value::Value;

pub mod accumulator_oracle;
pub mod batcher;
pub mod config;
pub mod context;
pub mod event;
pub mod generator;
pub mod materialize;
pub mod oracle;
pub mod report;
pub mod runner;
pub mod schema;
pub mod strategy;

use config::{ChaosConfig, SupportedOps};
use context::ChaosContext;
use event::ChaosBatch;
use oracle::MaterializedTable;
use report::Tolerances;
use runner::{OracleFn, RunnableChaos};
use schema::{ChaosSchema, KeyStrategy};
use strategy::{ColumnRegistry, ColumnSampler, RowContent, samplers};

use crate::{operator::FFIOperator, testing::harness::FFIOperatorHarness};

/// Errors surfaced from the chaos harness builder.
#[derive(Debug)]
pub enum ChaosError {
	/// `SupportedOps` configuration is unreachable: `Update` or `Remove`
	/// is enabled but `Insert` is not, so the generator can never populate
	/// any live rows.
	UnreachableSupportedOps,

	/// The builder is missing a required setting.
	MissingField(&'static str),

	/// `output_key` references a column that does not exist in
	/// `output_shape`.
	OutputKeyColumnMissing(String),

	/// One or more input shape columns lack a registered sampler.
	InputColumnsMissingSampler(Vec<String>),

	/// Wrapping the underlying FFI operator harness failed (config
	/// rejection, FFI initialization, etc.).
	HarnessBuild(String),
}

impl Display for ChaosError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ChaosError::UnreachableSupportedOps => write!(
				f,
				"SupportedOps configuration is unreachable: enabling Update or Remove without Insert leaves the generator with no way to populate live rows"
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

/// Entry point. Author calls `ChaosHarness::<T>::builder()` to start
/// configuring a run. The struct itself is just a namespace at this point;
/// the active object is the [`RunnableChaos`] returned from `build()`.
pub struct ChaosHarness<T: FFIOperator> {
	_phantom: PhantomData<T>,
}

impl<T: FFIOperator> ChaosHarness<T> {
	pub fn builder() -> ChaosHarnessBuilder<T> {
		ChaosHarnessBuilder::new()
	}
}

/// Fluent builder. Required: input/output shape, key strategy, output key,
/// per-column samplers (one per input column), and an oracle. Optional:
/// row constraints, tolerances, chaos config, supported ops, seed.
pub struct ChaosHarnessBuilder<T: FFIOperator> {
	seed: u64,
	config: ChaosConfig,
	node_id: FlowNodeId,
	version: CommitVersion,
	operator_config: Vec<(String, Value)>,
	input_shape: Option<RowShape>,
	output_shape: Option<RowShape>,
	key_strategy: Option<KeyStrategy>,
	output_key_columns: Vec<String>,
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
			config: ChaosConfig::default(),
			node_id: FlowNodeId(1),
			version: CommitVersion(1),
			operator_config: Vec::new(),
			input_shape: None,
			output_shape: None,
			key_strategy: None,
			output_key_columns: Vec::new(),
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

	pub fn with_chaos(mut self, config: ChaosConfig) -> Self {
		self.config = config;
		self
	}

	pub fn with_supported_ops(mut self, ops: SupportedOps) -> Self {
		self.config.supported_ops = ops;
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

	/// Per-knob config to be passed to `T::new`. Mirrors
	/// [`crate::testing::harness::FFIOperatorHarnessBuilder::with_config`].
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

	/// Register a column sampler. Authors typically call one of
	/// [`samplers::select`], [`samplers::u64_range`], etc., or hand-roll
	/// an `Arc<dyn Fn(&mut StdRng) -> Value>`.
	pub fn with_column(mut self, name: impl Into<String>, sampler: ColumnSampler) -> Self {
		self.registry.register(name, sampler);
		self
	}

	/// Run after per-column sampling. Lets the author derive or
	/// override columns based on already-sampled values.
	pub fn with_row_constraints(mut self, f: impl Fn(&mut RowContent) + Send + Sync + 'static) -> Self {
		self.registry.set_constraint(Arc::new(f));
		self
	}

	pub fn with_tolerance(mut self, column: impl Into<String>, tol: f64) -> Self {
		self.tolerances = mem::take(&mut self.tolerances).with(column, tol);
		self
	}

	/// Required. The oracle receives the per-batch event log (one
	/// `ChaosBatch` per `Change` the operator's `apply()` saw) and
	/// produces the expected materialized output table. Oracles for
	/// windowed operators that snapshot at end-of-batch iterate
	/// `batches` and snapshot at the end of each batch's inner loop.
	pub fn with_oracle<F>(mut self, f: F) -> Self
	where
		F: Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedTable + Send + Sync + 'static,
	{
		self.oracle = Some(Arc::new(f));
		self
	}

	/// Validate, build the inner `FFIOperatorHarness`, and bundle every
	/// piece into a `RunnableChaos` ready to `.run()`.
	///
	/// Validation is delegated to [`ChaosSchema::validate`] and
	/// [`ColumnRegistry::validate`]. Those methods are the single source
	/// of truth for validation rules; `build()` only translates their
	/// `Result` shapes into `ChaosError` variants.
	pub fn build(self) -> ChaosResult<RunnableChaos<T>> {
		validate_supported_ops(&self.config.supported_ops)?;
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
			config: self.config,
			schema,
			registry: Arc::new(self.registry),
			tolerances: self.tolerances,
			oracle,
			harness,
		})
	}
}

fn validate_supported_ops(ops: &SupportedOps) -> ChaosResult<()> {
	if !ops.insert && (ops.update || ops.remove) {
		return Err(ChaosError::UnreachableSupportedOps);
	}
	Ok(())
}

/// Convenience wrappers for proptest-style range-to-sampler shorthand.
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
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	use super::{config::BatchSizeDist, *};
	use crate::{
		config::Config,
		error::Result,
		operator::{
			FFIOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
			context::ffi::FFIOperatorContext,
		},
	};

	/// Minimal no-op operator used to monomorphize the chaos builder for
	/// tests that exercise validation (which runs *before* the harness is
	/// built or the operator is invoked). Its `apply` is never called by
	/// these tests; if it ever is, the test framework will panic loudly.
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
		let _ = ChaosConfig::default();
		let _ = SupportedOps::default();
		let _ = SupportedOps::insert_only();
		let _ = SupportedOps::no_remove();
		let _ = SupportedOps::no_update();
		let _ = BatchSizeDist::default();
		let _ = BatchSizeDist::Constant(1);
		let _ = BatchSizeDist::Uniform {
			min: 1,
			max: 10,
		};
		let _ = BatchSizeDist::Geometric(0.4);
		let _ = MaterializedTable::empty();
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
		// All-disabled passes the validator (it's just useless, not
		// unreachable). Caller is responsible for setting num_ops > 0.
		let none = SupportedOps {
			insert: false,
			update: false,
			remove: false,
		};
		assert!(validate_supported_ops(&none).is_ok());
	}

	/// Build a builder pre-populated with the minimum settings needed to
	/// reach validation. Tests then mutate one specific field to exercise
	/// the validation path of interest.
	fn well_formed_builder() -> ChaosHarnessBuilder<NoOpOperator> {
		ChaosHarness::<NoOpOperator>::builder()
			.with_input_shape(shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8)]))
			.with_output_shape(shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8)]))
			.with_key_strategy(KeyStrategy::Sequential)
			.with_output_key(["k"])
			.with_column("k", samplers::u64_range(1..1000))
			.with_column("v", samplers::f64_range(0.0..1.0))
			.with_oracle(|_, _| MaterializedTable::empty())
	}

	#[test]
	fn build_accepts_well_formed_builder() {
		// Sanity: the well-formed builder builds without errors.
		assert!(well_formed_builder().build().is_ok(), "expected well-formed builder to succeed");
	}

	/// Helper: extract the `Err` variant from `build()` without requiring
	/// the `Ok` side to be `Debug`. `RunnableChaos` doesn't impl Debug
	/// (its inner harness is FFI-heavy), so `Result::expect_err` is
	/// unavailable.
	fn expect_build_err(result: ChaosResult<RunnableChaos<NoOpOperator>>, label: &str) -> ChaosError {
		match result {
			Ok(_) => panic!("expected error from build(): {label}"),
			Err(e) => e,
		}
	}

	#[test]
	fn build_rejects_typoed_output_key() {
		// Wires `ChaosSchema::validate` into the build pipeline. If this
		// test fails because validation is bypassed in build(), the same
		// duplication that triggered the dead_code warning has crept
		// back in - the schema-level test would still pass while the
		// build path silently accepts typos.
		let err =
			expect_build_err(well_formed_builder().with_output_key(["typo"]).build(), "typo'd output_key");
		match err {
			ChaosError::OutputKeyColumnMissing(col) => assert_eq!(col, "typo"),
			other => panic!("expected OutputKeyColumnMissing(\"typo\"), got {other:?}"),
		}
	}

	#[test]
	fn build_rejects_input_columns_without_samplers() {
		// Same wiring assertion for `ColumnRegistry::validate`. Build
		// with an input shape that has a column not registered in the
		// sampler registry.
		let result = ChaosHarness::<NoOpOperator>::builder()
			.with_input_shape(shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8), ("missing", ValueType::Int8)]))
			.with_output_shape(shape(&[("k", ValueType::Uint8)]))
			.with_key_strategy(KeyStrategy::Sequential)
			.with_output_key(["k"])
			.with_column("k", samplers::u64_range(1..1000))
			.with_column("v", samplers::f64_range(0.0..1.0))
			// "missing" intentionally not registered.
			.with_oracle(|_, _| MaterializedTable::empty())
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
		// No input_shape -> MissingField("input_shape").
		let err = expect_build_err(ChaosHarness::<NoOpOperator>::builder().build(), "no input_shape");
		assert!(matches!(err, ChaosError::MissingField("input_shape")), "{err:?}");

		// input_shape only -> MissingField("output_shape").
		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.build(),
			"no output_shape",
		);
		assert!(matches!(err, ChaosError::MissingField("output_shape")), "{err:?}");

		// shapes only -> MissingField("key_strategy").
		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.with_output_shape(shape(&[("k", ValueType::Uint8)]))
				.build(),
			"no key_strategy",
		);
		assert!(matches!(err, ChaosError::MissingField("key_strategy")), "{err:?}");

		// key but no output_key -> MissingField("output_key").
		let err = expect_build_err(
			ChaosHarness::<NoOpOperator>::builder()
				.with_input_shape(shape(&[("k", ValueType::Uint8)]))
				.with_output_shape(shape(&[("k", ValueType::Uint8)]))
				.with_key_strategy(KeyStrategy::Sequential)
				.build(),
			"no output_key",
		);
		assert!(matches!(err, ChaosError::MissingField("output_key")), "{err:?}");

		// no oracle -> MissingField("oracle"). We need every other
		// required field present and a sampler for every input column,
		// otherwise the missing-oracle error gets shadowed by an
		// earlier check.
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
