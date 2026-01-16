// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM execution state.

use std::{collections::HashMap, sync::Arc};

use reifydb_rqlv2::{
	bytecode::program::{CompiledProgram, Constant, SortDirection},
	expression::types::{CompiledExpr, CompiledFilter},
};
use reifydb_type::value::{Value, blob::Blob, ordered_f64::OrderedF64};

use super::{
	context::VmContext,
	operand::{OperandValue, PipelineHandle},
	scope::ScopeChain,
	stack::CallStack,
};
#[cfg(feature = "trace")]
use crate::trace::tracer::VmTracer;
use crate::{
	error::{Result, VmError},
	operator::{
		scan_table::ScanState,
		sort::{SortOrder, SortSpec},
	},
	pipeline::Pipeline,
	trace::entry::TraceEntry,
};

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
	pub program: CompiledProgram,

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
	pub fn new(program: CompiledProgram, context: Arc<VmContext>) -> Self {
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
	pub fn take_trace(&mut self) -> Option<Vec<TraceEntry>> {
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
		match constant {
			Constant::Undefined => Value::Undefined,
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
