// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! VM execution state.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::{Column, Columns};
use reifydb_type::Value;

use super::{call_stack::CallStack, scope::ScopeChain};
use crate::{
	bytecode::Program,
	error::{Result, VmError},
	expr::{CompiledExpr, CompiledFilter},
	operator::sort::SortSpec,
	pipeline::Pipeline,
	source::SourceRegistry,
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
}

impl Default for VmConfig {
	fn default() -> Self {
		Self {
			max_operand_stack: 1024,
			max_pipeline_stack: 64,
			max_call_depth: 256,
			max_scope_depth: 256,
		}
	}
}

/// Execution context providing external resources.
pub struct VmContext {
	/// Source registry for table lookups.
	pub sources: Arc<dyn SourceRegistry>,

	/// VM configuration.
	pub config: VmConfig,

	/// Optional subquery executor for expression evaluation.
	pub subquery_executor: Option<Arc<dyn crate::expr::SubqueryExecutor>>,
}

impl VmContext {
	/// Create a new VM context.
	pub fn new(sources: Arc<dyn SourceRegistry>) -> Self {
		Self {
			sources,
			config: VmConfig::default(),
			subquery_executor: None,
		}
	}

	/// Create a new VM context with custom configuration.
	pub fn with_config(sources: Arc<dyn SourceRegistry>, config: VmConfig) -> Self {
		Self {
			sources,
			config,
			subquery_executor: None,
		}
	}

	/// Create a new VM context with a subquery executor.
	pub fn with_subquery_executor(
		sources: Arc<dyn SourceRegistry>,
		executor: Arc<dyn crate::expr::SubqueryExecutor>,
	) -> Self {
		Self {
			sources,
			config: VmConfig::default(),
			subquery_executor: Some(executor),
		}
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
	pub program: Arc<Program>,

	/// Execution context (sources, config).
	pub context: Arc<VmContext>,

	/// Pipeline registry (for pipeline handles).
	pipeline_registry: HashMap<u64, Pipeline>,
	next_pipeline_id: u64,
}

impl VmState {
	/// Create a new VM state.
	pub fn new(program: Arc<Program>, context: Arc<VmContext>) -> Self {
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
		}
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

	/// Get a constant string from the program.
	pub fn get_constant_string(&self, index: u16) -> Result<String> {
		match self.program.constants.get(index as usize) {
			Some(Value::Utf8(s)) => Ok(s.clone()),
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
	pub fn resolve_expr(&self, value: &OperandValue) -> Result<crate::expr::Expr> {
		match value {
			OperandValue::ExprRef(index) => self.program.expressions.get(*index as usize).cloned().ok_or(
				VmError::InvalidExpressionIndex {
					index: *index,
				},
			),
			_ => Err(VmError::ExpectedExpression),
		}
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
		match value {
			OperandValue::SortSpecRef(index) => {
				self.program.sort_specs.get(*index as usize).cloned().ok_or(
					VmError::InvalidSortSpecIndex {
						index: *index,
					},
				)
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
