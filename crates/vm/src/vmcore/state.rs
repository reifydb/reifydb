// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM execution state.

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::Catalog;
use reifydb_core::value::column::{Column, Columns};
use reifydb_rqlv2::{
	bytecode::{CompiledProgram, Constant},
	expression::{CompiledExpr, CompiledFilter},
};
use reifydb_type::Value;

#[cfg(feature = "trace")]
use super::trace::VmTracer;
use super::{call_stack::CallStack, scope::ScopeChain};
use crate::{
	error::{Result, VmError},
	operator::{ScanState, sort::SortSpec},
	pipeline::Pipeline,
};

/// A record is a single row with named fields.
#[derive(Debug, Clone)]
pub struct Record {
	/// Field name -> value pairs.
	pub fields: Vec<(String, Value)>,
}

impl Record {
	/// Create a new record from field name-value pairs.
	pub fn new(fields: Vec<(String, Value)>) -> Self {
		Self {
			fields,
		}
	}

	/// Get a field value by name.
	pub fn get(&self, name: &str) -> Option<&Value> {
		self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
	}
}

/// Values that can live on the operand stack.
#[derive(Debug, Clone)]
pub enum OperandValue {
	/// Scalar value (literals, computed results).
	Scalar(Value),

	/// Single column (vectorized value for columnar operations).
	Column(Column),

	/// Reference to an expression in the program.
	ExprRef(u16),

	/// Column reference by name.
	ColRef(String),

	/// List of column names (for select).
	ColList(Vec<String>),

	/// Materialized frame (collected pipeline result).
	Frame(Columns),

	/// Reference to a user-defined function.
	FunctionRef(u16),

	/// Pipeline reference (for storing pipelines in variables).
	PipelineRef(PipelineHandle),

	/// Sort specification reference.
	SortSpecRef(u16),

	/// Extension specification reference.
	ExtSpecRef(u16),

	/// Record (single row with named fields).
	Record(Record),
}

impl OperandValue {
	/// Check if this is a scalar value.
	pub fn is_scalar(&self) -> bool {
		matches!(self, OperandValue::Scalar(_))
	}

	/// Check if this is a column value.
	pub fn is_column(&self) -> bool {
		matches!(self, OperandValue::Column(_))
	}

	/// Try to get as a scalar value.
	pub fn as_scalar(&self) -> Option<&Value> {
		match self {
			OperandValue::Scalar(v) => Some(v),
			_ => None,
		}
	}

	/// Try to get as a column.
	pub fn as_column(&self) -> Option<&Column> {
		match self {
			OperandValue::Column(c) => Some(c),
			_ => None,
		}
	}

	/// Try to take as a column (consumes self).
	pub fn into_column(self) -> Option<Column> {
		match self {
			OperandValue::Column(c) => Some(c),
			_ => None,
		}
	}

	/// Try to get as an integer.
	pub fn as_int(&self) -> Option<i64> {
		match self {
			OperandValue::Scalar(Value::Int8(n)) => Some(*n),
			_ => None,
		}
	}

	/// Try to get as a boolean.
	pub fn as_bool(&self) -> Option<bool> {
		match self {
			OperandValue::Scalar(Value::Boolean(b)) => Some(*b),
			_ => None,
		}
	}

	/// Try to get as a string.
	pub fn as_string(&self) -> Option<&str> {
		match self {
			OperandValue::Scalar(Value::Utf8(s)) => Some(s),
			_ => None,
		}
	}
}

/// Handle to a pipeline (can be cloned, represents shared ownership).
#[derive(Debug, Clone)]
pub struct PipelineHandle {
	/// Unique identifier for lookup in pipeline registry.
	pub id: u64,
}

/// VM configuration.
#[derive(Debug, Clone)]
pub struct VmConfig {
	/// Maximum operand stack depth.
	pub max_operand_stack: usize,

	/// Maximum pipeline stack depth.
	pub max_pipeline_stack: usize,

	/// Maximum call stack depth.
	pub max_call_depth: usize,

	/// Maximum scope depth.
	pub max_scope_depth: usize,

	/// Batch size for table scans.
	pub batch_size: u64,
}

impl Default for VmConfig {
	fn default() -> Self {
		Self {
			max_operand_stack: 1024,
			max_pipeline_stack: 64,
			max_call_depth: 256,
			max_scope_depth: 256,
			batch_size: 1000,
		}
	}
}

/// Execution context providing external resources.
pub struct VmContext {
	/// VM configuration.
	pub config: VmConfig,

	/// Optional catalog for real storage lookups.
	pub catalog: Option<Catalog>,
}

impl VmContext {
	/// Create a new VM context with default configuration.
	pub fn new() -> Self {
		Self {
			config: VmConfig::default(),
			catalog: None,
		}
	}

	/// Create a new VM context with custom configuration.
	pub fn with_config(config: VmConfig) -> Self {
		Self {
			config,
			catalog: None,
		}
	}

	/// Create a new VM context with a catalog.
	pub fn with_catalog(catalog: Catalog) -> Self {
		Self {
			config: VmConfig::default(),
			catalog: Some(catalog),
		}
	}

	/// Create a new VM context with both custom config and catalog.
	pub fn with_config_and_catalog(config: VmConfig, catalog: Catalog) -> Self {
		Self {
			config,
			catalog: Some(catalog),
		}
	}
}

impl Default for VmContext {
	fn default() -> Self {
		Self::new()
	}
}

/// Main VM execution state.
pub struct VmState {
	/// Instruction pointer.
	pub ip: usize,

	/// Operand stack.
	pub operand_stack: Vec<OperandValue>,

	/// Pipeline stack.
	pub pipeline_stack: Vec<Pipeline>,

	/// Variable scopes.
	pub scopes: ScopeChain,

	/// Function call stack.
	pub call_stack: CallStack,

	/// The program being executed.
	pub program: Arc<CompiledProgram>,

	/// Execution context (sources, config).
	pub context: Arc<VmContext>,

	/// Pipeline registry (for pipeline handles).
	pipeline_registry: HashMap<u64, Pipeline>,
	next_pipeline_id: u64,

	/// Active table scans (source_index -> ScanState).
	/// Stores iteration state for batch-at-a-time table scans.
	pub active_scans: HashMap<u16, ScanState>,

	/// Internal (compiler-generated) variables storage.
	/// Used for loop indices, temporary values, etc.
	pub internal_vars: HashMap<u16, OperandValue>,

	/// Optional tracer for recording execution.
	#[cfg(feature = "trace")]
	pub tracer: Option<VmTracer>,
}

impl VmState {
	/// Create a new VM state.
	pub fn new(program: Arc<CompiledProgram>, context: Arc<VmContext>) -> Self {
		let max_call_depth = context.config.max_call_depth;
		Self {
			ip: program.entry_point,
			operand_stack: Vec::new(),
			pipeline_stack: Vec::new(),
			scopes: ScopeChain::new(),
			call_stack: CallStack::new(max_call_depth),
			program,
			context,
			pipeline_registry: HashMap::new(),
			next_pipeline_id: 0,
			active_scans: HashMap::new(),
			internal_vars: HashMap::new(),
			#[cfg(feature = "trace")]
			tracer: None,
		}
	}

	/// Enable tracing with the given tracer.
	#[cfg(feature = "trace")]
	pub fn with_tracer(mut self, tracer: VmTracer) -> Self {
		self.tracer = Some(tracer);
		self
	}

	/// Take the trace entries after execution.
	#[cfg(feature = "trace")]
	pub fn take_trace(&mut self) -> Option<Vec<super::trace::TraceEntry>> {
		self.tracer.take().map(|t| t.take_entries())
	}

	/// Push a value onto the operand stack.
	pub fn push_operand(&mut self, value: OperandValue) -> Result<()> {
		if self.operand_stack.len() >= self.context.config.max_operand_stack {
			return Err(VmError::StackOverflow {
				stack: "operand".into(),
			});
		}
		self.operand_stack.push(value);
		Ok(())
	}

	/// Pop a value from the operand stack.
	pub fn pop_operand(&mut self) -> Result<OperandValue> {
		self.operand_stack.pop().ok_or(VmError::StackUnderflow {
			stack: "operand".into(),
		})
	}

	/// Peek at the top of the operand stack.
	pub fn peek_operand(&self) -> Result<&OperandValue> {
		self.operand_stack.last().ok_or(VmError::StackUnderflow {
			stack: "operand".into(),
		})
	}

	/// Push a pipeline onto the pipeline stack.
	pub fn push_pipeline(&mut self, pipeline: Pipeline) -> Result<()> {
		if self.pipeline_stack.len() >= self.context.config.max_pipeline_stack {
			return Err(VmError::StackOverflow {
				stack: "pipeline".into(),
			});
		}
		self.pipeline_stack.push(pipeline);
		Ok(())
	}

	/// Pop a pipeline from the pipeline stack.
	pub fn pop_pipeline(&mut self) -> Result<Pipeline> {
		self.pipeline_stack.pop().ok_or(VmError::StackUnderflow {
			stack: "pipeline".into(),
		})
	}

	/// Register a pipeline and get a handle for it.
	pub fn register_pipeline(&mut self, pipeline: Pipeline) -> PipelineHandle {
		let id = self.next_pipeline_id;
		self.next_pipeline_id += 1;
		self.pipeline_registry.insert(id, pipeline);
		PipelineHandle {
			id,
		}
	}

	/// Get and remove a pipeline by handle.
	pub fn take_pipeline(&mut self, handle: &PipelineHandle) -> Option<Pipeline> {
		self.pipeline_registry.remove(&handle.id)
	}

	/// Convert an RQLv2 Constant to a VM Value.
	fn constant_to_value(constant: &Constant) -> Value {
		use reifydb_type::value::{Blob, OrderedF64};

		match constant {
			Constant::Null => Value::Undefined,
			Constant::Bool(b) => Value::Boolean(*b),
			Constant::Int(i) => Value::Int8(*i),
			Constant::Float(f) => {
				Value::Float8(OrderedF64::try_from(*f).expect("RQLv2 constants should not contain NaN"))
			}
			Constant::String(s) => Value::Utf8(s.clone()),
			Constant::Bytes(b) => Value::Blob(Blob::from(b.clone())),
		}
	}

	/// Get a constant value from the program.
	pub fn get_constant(&self, index: u16) -> Result<Value> {
		self.program.constants.get(index as usize).map(Self::constant_to_value).ok_or(
			VmError::InvalidConstantIndex {
				index,
			},
		)
	}

	/// Get a constant string from the program.
	pub fn get_constant_string(&self, index: u16) -> Result<String> {
		match self.program.constants.get(index as usize) {
			Some(Constant::String(s)) => Ok(s.clone()),
			Some(_) => Err(VmError::ExpectedString {
				index,
			}),
			None => Err(VmError::InvalidConstantIndex {
				index,
			}),
		}
	}

	/// Check if a value is truthy.
	pub fn is_truthy(&self, value: &OperandValue) -> Result<bool> {
		match value {
			OperandValue::Scalar(Value::Boolean(b)) => Ok(*b),
			OperandValue::Scalar(Value::Undefined) => Ok(false),
			OperandValue::Scalar(Value::Int8(n)) => Ok(*n != 0),
			_ => Err(VmError::ExpectedBoolean),
		}
	}

	/// Resolve an expression reference to an expression.
	///
	/// DEPRECATED: RQLv2 uses pre-compiled expressions (CompiledExpr/CompiledFilter)
	/// instead of AST expressions. This method will be removed.
	#[allow(dead_code)]
	pub fn resolve_expr(&self, _value: &OperandValue) -> Result<()> {
		// RQLv2's CompiledProgram doesn't store AST expressions.
		// Expressions are pre-compiled to closures in compiled_exprs/compiled_filters.
		Err(VmError::UnsupportedOperation {
			operation: "Expression references (RQLv2 uses pre-compiled expressions)".into(),
		})
	}

	/// Resolve a compiled filter reference.
	pub fn resolve_compiled_filter(&self, value: &OperandValue) -> Result<CompiledFilter> {
		match value {
			OperandValue::ExprRef(index) => {
				self.program.compiled_filters.get(*index as usize).cloned().ok_or(
					VmError::InvalidExpressionIndex {
						index: *index,
					},
				)
			}
			_ => Err(VmError::ExpectedExpression),
		}
	}

	/// Resolve a compiled expression reference.
	pub fn resolve_compiled_expr(&self, value: &OperandValue) -> Result<CompiledExpr> {
		match value {
			OperandValue::ExprRef(index) => {
				self.program.compiled_exprs.get(*index as usize).cloned().ok_or(
					VmError::InvalidExpressionIndex {
						index: *index,
					},
				)
			}
			_ => Err(VmError::ExpectedExpression),
		}
	}

	/// Resolve a column list value.
	pub fn resolve_col_list(&self, value: &OperandValue) -> Result<Vec<String>> {
		match value {
			OperandValue::ColList(cols) => Ok(cols.clone()),
			_ => Err(VmError::ExpectedColumnList),
		}
	}

	/// Resolve an integer value.
	pub fn resolve_int(&self, value: &OperandValue) -> Result<i64> {
		match value {
			OperandValue::Scalar(Value::Int8(n)) => Ok(*n),
			_ => Err(VmError::ExpectedInteger),
		}
	}

	/// Resolve a sort specification.
	pub fn resolve_sort_spec(&self, value: &OperandValue) -> Result<Vec<SortSpec>> {
		use reifydb_rqlv2::bytecode::SortDirection;

		use crate::operator::sort::SortOrder;

		match value {
			OperandValue::SortSpecRef(index) => {
				let rql_spec = self.program.sort_specs.get(*index as usize).ok_or(
					VmError::InvalidSortSpecIndex {
						index: *index,
					},
				)?;

				// Convert RQLv2's SortSpec (has Vec<SortKey>) to Vec<VM SortSpec>
				let vm_specs: Vec<SortSpec> = rql_spec
					.keys
					.iter()
					.map(|key| SortSpec {
						column: key.column.clone(),
						order: match key.direction {
							SortDirection::Asc => SortOrder::Asc,
							SortDirection::Desc => SortOrder::Desc,
						},
					})
					.collect();

				Ok(vm_specs)
			}
			_ => Err(VmError::ExpectedSortSpec),
		}
	}

	/// Resolve an extension specification to compiled expressions.
	pub fn resolve_extension_spec(&self, value: &OperandValue) -> Result<Vec<(String, CompiledExpr)>> {
		match value {
			OperandValue::ExtSpecRef(index) => {
				let spec = self.program.extension_specs.get(*index as usize).ok_or(
					VmError::InvalidExtSpecIndex {
						index: *index,
					},
				)?;

				spec.iter()
					.map(|(name, expr_index)| {
						let compiled = self
							.program
							.compiled_exprs
							.get(*expr_index as usize)
							.cloned()
							.ok_or(VmError::InvalidExpressionIndex {
								index: *expr_index,
							})?;
						Ok((name.clone(), compiled))
					})
					.collect()
			}
			_ => Err(VmError::ExpectedExtensionSpec),
		}
	}
}
