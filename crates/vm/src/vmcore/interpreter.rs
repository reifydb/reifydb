// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode interpreter.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::ColumnData;
use reifydb_engine::StandardTransaction;
use reifydb_rqlv2::{
	bytecode::{BytecodeReader, Opcode, OperatorKind},
	expression::{EvalContext, EvalValue},
};

use super::{
	builtin::BuiltinRegistry,
	call_stack::CallFrame,
	script::BytecodeScriptCaller,
	state::{OperandValue, VmState},
};
use crate::{
	error::{Result, VmError},
	operator::{FilterOp, ProjectOp, ScanTableOp, SelectOp, SortOp, TakeOp},
	pipeline::Pipeline,
};

/// Result of dispatching a single instruction.
pub enum DispatchResult {
	/// Continue to next instruction.
	Continue,

	/// Halt execution.
	Halt,

	/// Yield a pipeline result (for top-level expression).
	Yield(Pipeline),
}

impl VmState {
	/// Execute the program until halt or yield.
	pub async fn execute<'a>(&mut self, rx: &mut StandardTransaction<'a>) -> Result<Option<Pipeline>> {
		loop {
			let result = self.step(Some(rx)).await?;
			match result {
				DispatchResult::Continue => continue,
				DispatchResult::Halt => break,
				DispatchResult::Yield(pipeline) => return Ok(Some(pipeline)),
			}
		}

		// Return top of pipeline stack if present
		Ok(self.pipeline_stack.pop())
	}

	/// Execute a single instruction.
	///
	/// The transaction is optional - if None, only in-memory sources can be used.
	pub async fn step<'a>(&mut self, rx: Option<&mut StandardTransaction<'a>>) -> Result<DispatchResult> {
		// Helper macros for reading operands
		macro_rules! read_u8 {
			($reader:expr) => {
				$reader.read_u8().ok_or(VmError::UnexpectedEndOfBytecode)?
			};
		}

		macro_rules! read_u16 {
			($reader:expr) => {
				$reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?
			};
		}

		macro_rules! read_i16 {
			($reader:expr) => {
				$reader.read_i16().ok_or(VmError::UnexpectedEndOfBytecode)?
			};
		}

		macro_rules! read_u32 {
			($reader:expr) => {
				$reader.read_u32().ok_or(VmError::UnexpectedEndOfBytecode)?
			};
		}

		// Set up bytecode reader at current IP
		let mut reader = BytecodeReader::new(&self.program.bytecode);
		reader.set_position(self.ip);

		// Read the opcode
		let opcode = reader.read_opcode().ok_or(VmError::InvalidBytecode {
			position: self.ip,
		})?;

		match opcode {
			// ─────────────────────────────────────────────────────────
			// Stack Operations
			// ─────────────────────────────────────────────────────────
			Opcode::PushConst => {
				let index = read_u16!(reader);
				let next_ip = reader.position();
				let value = self.get_constant(index)?;
				self.push_operand(OperandValue::Scalar(value))?;
				self.ip = next_ip;
			}

			Opcode::PushExpr => {
				let index = read_u16!(reader);
				let next_ip = reader.position();
				self.push_operand(OperandValue::ExprRef(index))?;
				self.ip = next_ip;
			}

			Opcode::PushColRef => {
				let name_index = read_u16!(reader);
				let next_ip = reader.position();
				let name = self.get_constant_string(name_index)?;
				self.push_operand(OperandValue::ColRef(name))?;
				self.ip = next_ip;
			}

			Opcode::PushColList => {
				let index = read_u16!(reader);
				let next_ip = reader.position();
				let columns = self.program.column_lists.get(index as usize).cloned().ok_or(
					VmError::InvalidColumnListIndex {
						index,
					},
				)?;
				self.push_operand(OperandValue::ColList(columns))?;
				self.ip = next_ip;
			}

			Opcode::PushSortSpec => {
				let index = read_u16!(reader);
				let next_ip = reader.position();
				self.push_operand(OperandValue::SortSpecRef(index))?;
				self.ip = next_ip;
			}

			Opcode::PushExtSpec => {
				let index = read_u16!(reader);
				let next_ip = reader.position();
				self.push_operand(OperandValue::ExtSpecRef(index))?;
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Variable Operations (by ID)
			// ─────────────────────────────────────────────────────────
			Opcode::LoadVar => {
				let var_id = read_u32!(reader);
				let next_ip = reader.position();
				let value =
					self.scopes.get_by_id(var_id).cloned().ok_or(VmError::UndefinedVariable {
						name: format!("${}", var_id),
					})?;
				self.push_operand(value)?;
				self.ip = next_ip;
			}

			Opcode::StoreVar => {
				let var_id = read_u32!(reader);
				let next_ip = reader.position();
				let value = self.pop_operand()?;
				self.scopes.set_by_id(var_id, value);
				self.ip = next_ip;
			}

			Opcode::UpdateVar => {
				let var_id = read_u32!(reader);
				let next_ip = reader.position();
				let value = self.pop_operand()?;
				// Update existing variable (searches all scopes)
				if !self.scopes.update_by_id(var_id, value) {
					return Err(VmError::UndefinedVariable {
						name: format!("${}", var_id),
					});
				}
				self.ip = next_ip;
			}

			Opcode::LoadPipeline => {
				let var_id = read_u32!(reader);
				let next_ip = reader.position();
				let value =
					self.scopes.get_by_id(var_id).cloned().ok_or(VmError::UndefinedVariable {
						name: format!("${}", var_id),
					})?;

				match value {
					OperandValue::PipelineRef(handle) => {
						let pipeline = self
							.take_pipeline(&handle)
							.ok_or(VmError::InvalidPipelineHandle)?;
						self.push_pipeline(pipeline)?;
					}
					_ => return Err(VmError::ExpectedPipeline),
				}
				self.ip = next_ip;
			}

			Opcode::StorePipeline => {
				let var_id = read_u32!(reader);
				let next_ip = reader.position();
				let pipeline = self.pop_pipeline()?;
				let handle = self.register_pipeline(pipeline);
				self.scopes.set_by_id(var_id, OperandValue::PipelineRef(handle));
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Internal Variable Operations
			// ─────────────────────────────────────────────────────────
			Opcode::LoadInternalVar => {
				let var_id = read_u16!(reader);
				let next_ip = reader.position();
				let value =
					self.internal_vars.get(&var_id).cloned().ok_or(VmError::UndefinedVariable {
						name: format!("__internal_{}", var_id),
					})?;
				self.push_operand(value)?;
				self.ip = next_ip;
			}

			Opcode::StoreInternalVar => {
				let var_id = read_u16!(reader);
				let next_ip = reader.position();
				let value = self.pop_operand()?;
				self.internal_vars.insert(var_id, value);
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Pipeline Operations
			// ─────────────────────────────────────────────────────────
			Opcode::Source => {
				let source_index = read_u16!(reader);
				let next_ip = reader.position();
				let source_def = self.program.sources.get(source_index as usize).ok_or(
					VmError::InvalidSourceIndex {
						index: source_index,
					},
				)?;

				if let (Some(catalog), Some(rx)) = (&self.context.catalog, rx) {
					// 1. Initialize scan state
					let op = ScanTableOp::new(
						source_def.name.clone(),
						self.context.config.batch_size,
					);
					let mut scan_state = op.initialize(catalog, rx).await?;

					// 2. Fetch first batch to maintain backward compatibility
					let batch_size = self.context.config.batch_size;
					let batch_opt =
						ScanTableOp::next_batch(&mut scan_state, rx, batch_size).await?;

					// 3. Store scan state (for potential future FetchBatch calls)
					self.active_scans.insert(source_index, scan_state);

					// 4. Push first batch as pipeline (maintains old semantics)
					let pipeline = if let Some(batch) = batch_opt {
						crate::pipeline::from_batch(batch)
					} else {
						crate::pipeline::empty()
					};
					self.push_pipeline(pipeline)?;

					self.ip = next_ip;
				} else {
					return Err(VmError::TableNotFound {
						name: source_def.name.clone(),
					});
				}
			}

			Opcode::Inline => {
				let next_ip = reader.position();
				let pipeline: Pipeline = Box::pin(futures_util::stream::empty());
				self.push_pipeline(pipeline)?;
				self.ip = next_ip;
			}

			Opcode::Apply => {
				let op_kind_byte = read_u8!(reader);
				let next_ip = reader.position();
				let op_kind = OperatorKind::try_from(op_kind_byte).map_err(|_| {
					VmError::UnknownOperatorKind {
						kind: op_kind_byte,
					}
				})?;
				self.apply_operator(op_kind)?;
				self.ip = next_ip;
			}

			Opcode::Collect => {
				let next_ip = reader.position();
				let pipeline = self.pop_pipeline()?;
				let columns = crate::pipeline::collect(pipeline).await?;
				self.push_operand(OperandValue::Frame(columns))?;
				self.ip = next_ip;
			}

			Opcode::PopPipeline => {
				let next_ip = reader.position();
				let _ = self.pop_pipeline()?;
				self.ip = next_ip;
			}

			Opcode::Merge => {
				return Err(VmError::UnsupportedOperation {
					operation: "Merge".to_string(),
				});
			}

			Opcode::FetchBatch => {
				let source_index = read_u16!(reader);
				let next_ip = reader.position();
				// Fetch next batch from active scan
				if let Some(rx) = rx {
					let scan_state = self
						.active_scans
						.get_mut(&source_index)
						.ok_or(VmError::Internal("scan not initialized".to_string()))?;

					let batch_size = self.context.config.batch_size;
					let batch_opt = ScanTableOp::next_batch(scan_state, rx, batch_size).await?;

					if let Some(batch) = batch_opt {
						// Has more data - push batch and true
						self.push_pipeline(crate::pipeline::from_batch(batch))?;
						self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(
							true,
						)))?;
					} else {
						// Exhausted - push empty pipeline and false
						self.push_pipeline(crate::pipeline::empty())?;
						self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(
							false,
						)))?;
					}
					self.ip = next_ip;
				} else {
					return Err(VmError::Internal("FetchBatch requires transaction".to_string()));
				}
			}

			Opcode::CheckComplete => {
				let next_ip = reader.position();
				// Pop boolean from operand stack (query complete flag)
				// This is used by TAKE and other limiting operators to signal completion
				// The VM could use this in the future to stop execution early
				let _complete = match self.pop_operand()? {
					OperandValue::Scalar(reifydb_type::Value::Boolean(b)) => b,
					_ => return Err(VmError::ExpectedBoolean),
				};

				// For now, just consume the flag
				// In the future, this could set a flag on VmState to enable early termination
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Control Flow
			// ─────────────────────────────────────────────────────────
			Opcode::Jump => {
				let offset = read_i16!(reader);
				let next_ip = reader.position();
				self.ip = (next_ip as i32 + offset as i32) as usize;
			}

			Opcode::JumpIf => {
				let offset = read_i16!(reader);
				let next_ip = reader.position();
				let value = self.pop_operand()?;
				if self.is_truthy(&value)? {
					self.ip = (next_ip as i32 + offset as i32) as usize;
				} else {
					self.ip = next_ip;
				}
			}

			Opcode::JumpIfNot => {
				let offset = read_i16!(reader);
				let next_ip = reader.position();
				let value = self.pop_operand()?;
				if !self.is_truthy(&value)? {
					self.ip = (next_ip as i32 + offset as i32) as usize;
				} else {
					self.ip = next_ip;
				}
			}

			// ─────────────────────────────────────────────────────────
			// Function Calls
			// ─────────────────────────────────────────────────────────
			Opcode::Call => {
				let func_index = read_u16!(reader);
				let next_ip = reader.position();

				let func_def = self.program.script_functions.get(func_index as usize).ok_or(
					VmError::InvalidFunctionIndex {
						index: func_index,
					},
				)?;

				// Push call frame
				let frame = CallFrame::new(
					func_index,
					next_ip,
					self.operand_stack.len(),
					self.pipeline_stack.len(),
					self.scopes.depth(),
				);

				if !self.call_stack.push(frame) {
					return Err(VmError::StackOverflow {
						stack: "call".into(),
					});
				}

				// Jump to function body (scope management is done within the function bytecode)
				self.ip = func_def.bytecode_offset;
			}

			Opcode::Return => {
				// Check if we're at the top level (no call frames)
				if self.call_stack.is_empty() {
					// Top-level return: yield the pipeline if present
					if let Some(pipeline) = self.pipeline_stack.pop() {
						return Ok(DispatchResult::Yield(pipeline));
					} else {
						// No pipeline to return, just halt
						return Ok(DispatchResult::Halt);
					}
				}

				// Inside a function: pop call frame and return to caller
				let frame = self.call_stack.pop().unwrap();

				// Restore scope
				self.scopes.pop_to_depth(frame.scope_depth);

				// Clean up operand stack (keep return value if any)
				let return_value = if self.operand_stack.len() > frame.operand_base {
					self.operand_stack.pop()
				} else {
					None
				};
				self.operand_stack.truncate(frame.operand_base);
				if let Some(value) = return_value {
					self.push_operand(value)?;
				}

				// Return to caller
				self.ip = frame.return_address;
			}

			Opcode::CallBuiltin => {
				let name_index = read_u16!(reader);
				let arg_count = read_u8!(reader) as usize;
				let next_ip = reader.position();

				// Get function name from constant pool
				let func_name = self.get_constant_string(name_index)?;

				// Pop arguments from stack (in reverse order)
				let mut args = Vec::with_capacity(arg_count);
				for _ in 0..arg_count {
					args.push(self.pop_operand()?);
				}
				args.reverse();

				// Look up and execute builtin
				let registry = BuiltinRegistry::new();
				if let Some(result) = registry.call(&func_name, &args)? {
					self.push_operand(result)?;
				}

				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Scope Management
			// ─────────────────────────────────────────────────────────
			Opcode::EnterScope => {
				let next_ip = reader.position();
				self.scopes.push();
				self.ip = next_ip;
			}

			Opcode::ExitScope => {
				let next_ip = reader.position();
				self.scopes.pop();
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Frame/Record Operations
			// ─────────────────────────────────────────────────────────
			Opcode::FrameLen => {
				let next_ip = reader.position();
				let frame = self.pop_operand()?;
				match frame {
					OperandValue::Frame(columns) => {
						let len = columns.row_count() as i64;
						self.push_operand(OperandValue::Scalar(reifydb_type::Value::Int8(
							len,
						)))?;
					}
					_ => return Err(VmError::ExpectedFrame),
				}
				self.ip = next_ip;
			}

			Opcode::FrameRow => {
				let next_ip = reader.position();
				let index = self.pop_operand()?;
				let frame = self.pop_operand()?;

				let row_index = match index {
					OperandValue::Scalar(reifydb_type::Value::Int8(n)) => n as usize,
					_ => return Err(VmError::ExpectedInteger),
				};

				match frame {
					OperandValue::Frame(columns) => {
						// Build a Record from the row at the given index
						let mut fields = Vec::new();
						for col in columns.iter() {
							let name = col.name().text().to_string();
							let value = col.data().get_value(row_index);
							fields.push((name, value));
						}
						self.push_operand(OperandValue::Record(super::state::Record::new(
							fields,
						)))?;
					}
					_ => return Err(VmError::ExpectedFrame),
				}
				self.ip = next_ip;
			}

			Opcode::GetField => {
				let name_index = read_u16!(reader);
				let next_ip = reader.position();
				let record = self.pop_operand()?;
				let field_name = self.get_constant_string(name_index)?;

				match record {
					OperandValue::Record(rec) => {
						let value = rec
							.get(&field_name)
							.cloned()
							.unwrap_or(reifydb_type::Value::Undefined);
						self.push_operand(OperandValue::Scalar(value))?;
					}
					_ => return Err(VmError::ExpectedRecord),
				}
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Scalar Arithmetic and Comparison
			// ─────────────────────────────────────────────────────────
			Opcode::IntAdd => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a + b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Int8(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntLt => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a < b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntEq => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a == b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntSub => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a - b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Int8(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntMul => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a * b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Int8(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntDiv => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => {
						if b == 0 {
							return Err(VmError::DivisionByZero);
						}
						a / b
					}
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Int8(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntNe => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a != b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntLe => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a <= b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntGt => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a > b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(result)))?;
				self.ip = next_ip;
			}

			Opcode::IntGe => {
				let next_ip = reader.position();
				let b = self.pop_operand()?;
				let a = self.pop_operand()?;

				let result = match (a, b) {
					(
						OperandValue::Scalar(reifydb_type::Value::Int8(a)),
						OperandValue::Scalar(reifydb_type::Value::Int8(b)),
					) => a >= b,
					_ => return Err(VmError::ExpectedInteger),
				};

				self.push_operand(OperandValue::Scalar(reifydb_type::Value::Boolean(result)))?;
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Columnar Operations
			// ─────────────────────────────────────────────────────────
			Opcode::ColAdd
			| Opcode::ColSub
			| Opcode::ColMul
			| Opcode::ColDiv
			| Opcode::ColLt
			| Opcode::ColLe
			| Opcode::ColGt
			| Opcode::ColGe
			| Opcode::ColEq
			| Opcode::ColNe
			| Opcode::ColAnd
			| Opcode::ColOr => {
				return Err(VmError::UnsupportedOperation {
					operation: "columnar binary operation (not yet implemented)".to_string(),
				});
			}

			Opcode::ColNot => {
				return Err(VmError::UnsupportedOperation {
					operation: "columnar NOT operation (not yet implemented)".to_string(),
				});
			}

			// ─────────────────────────────────────────────────────────
			// Control
			// ─────────────────────────────────────────────────────────
			Opcode::Nop => {
				let next_ip = reader.position();
				self.ip = next_ip;
			}

			Opcode::Halt => {
				return Ok(DispatchResult::Halt);
			}

			// DDL/DML opcodes not yet implemented
			_ => {
				return Err(VmError::UnsupportedOperation {
					operation: format!("Opcode {:?} not yet implemented", opcode),
				});
			}
		}

		Ok(DispatchResult::Continue)
	}

	/// Apply an operator to the top pipeline.
	fn apply_operator(&mut self, op_kind: OperatorKind) -> Result<()> {
		let pipeline = self.pop_pipeline()?;

		let new_pipeline = match op_kind {
			OperatorKind::Filter => {
				let expr_ref = self.pop_operand()?;
				let compiled = self.resolve_compiled_filter(&expr_ref)?;
				let eval_ctx = self.capture_scope_context();
				FilterOp::with_context(compiled, eval_ctx).apply(pipeline)
			}

			OperatorKind::Select => {
				let col_list = self.pop_operand()?;
				let columns = self.resolve_col_list(&col_list)?;
				SelectOp::new(columns).apply(pipeline)
			}

			OperatorKind::Extend => {
				let spec = self.pop_operand()?;
				let extensions = self.resolve_extension_spec(&spec)?;
				let eval_ctx = self.capture_scope_context();
				ProjectOp::extend_with_context(extensions, eval_ctx).apply(pipeline)
			}

			OperatorKind::Take => {
				let limit = self.pop_operand()?;
				let n = self.resolve_int(&limit)?;
				TakeOp::new(n as usize).apply(pipeline)
			}

			OperatorKind::Sort => {
				let spec = self.pop_operand()?;
				let sort_spec = self.resolve_sort_spec(&spec)?;
				SortOp::new(sort_spec).apply(pipeline)
			}

			// Not yet implemented
			_ => {
				return Err(VmError::UnsupportedOperation {
					operation: format!("OperatorKind {:?} not yet implemented", op_kind),
				});
			}
		};

		self.push_pipeline(new_pipeline)?;
		Ok(())
	}

	/// Capture all scope variables into an EvalContext for expression evaluation.
	///
	/// This method creates an EvalContext with a BytecodeScriptCaller that can
	/// execute script functions by running their bytecode.
	fn capture_scope_context(&self) -> EvalContext {
		// Create a script function caller that can execute bytecode
		let caller = Arc::new(BytecodeScriptCaller::new(self.program.clone()));
		EvalContext::with_script_functions(caller)
	}

	/// Build a function executor from the program's bytecode functions.
	///
	/// DEPRECATED: User-defined functions are not supported in RQLv2 yet.
	/// This method will be removed when DSL compilation is replaced with RQLv2.
	#[allow(dead_code)]
	fn build_function_executor(&self) {
		// Stubbed out - RQLv2 doesn't support user-defined functions yet
	}
}

/// Convert an OperandValue to an EvalValue if possible.
///
/// DEPRECATED: This function is no longer used since the old DSL module is deprecated.
#[allow(dead_code)]
fn operand_to_eval_value(value: &OperandValue) -> Option<EvalValue> {
	match value {
		OperandValue::Scalar(v) => Some(EvalValue::Scalar(v.clone())),
		OperandValue::Record(r) => {
			// Convert Record to HashMap for RQLv2's EvalValue
			let mut map = HashMap::new();
			for (name, val) in &r.fields {
				map.insert(name.clone(), val.clone());
			}
			Some(EvalValue::Record(map))
		}
		_ => None, // Other types cannot be used in expressions
	}
}

/// Broadcast a scalar value to a column with the given row count.
#[allow(dead_code)]
fn broadcast_scalar_to_column(value: &reifydb_type::Value, row_count: usize) -> ColumnData {
	match value {
		reifydb_type::Value::Boolean(b) => ColumnData::bool(vec![*b; row_count]),
		reifydb_type::Value::Int8(n) => ColumnData::int8(vec![*n; row_count]),
		reifydb_type::Value::Float8(f) => ColumnData::float8(vec![f.value(); row_count]),
		reifydb_type::Value::Utf8(s) => ColumnData::utf8(vec![s.clone(); row_count]),
		reifydb_type::Value::Undefined => ColumnData::int8(vec![0; row_count]),
		_ => ColumnData::int8(vec![0; row_count]),
	}
}

// TODO: User-defined function execution needs to be refactored to work with transactions.
// For now, function execution in expressions is not supported when functions contain
// table access operations. Pure computation functions will need a different execution path.

// /// Simple blocking executor for futures that complete immediately.
// fn block_on_simple<F: std::future::Future>(fut: F) -> F::Output {
// 	use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
//
// 	fn noop_clone(_: *const ()) -> RawWaker {
// 		noop_raw_waker()
// 	}
// 	fn noop(_: *const ()) {}
// 	fn noop_raw_waker() -> RawWaker {
// 		static VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
// 		RawWaker::new(std::ptr::null(), &VTABLE)
// 	}
//
// 	let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
// 	let mut cx = Context::from_waker(&waker);
// 	let mut fut = std::pin::pin!(fut);
//
// 	loop {
// 		match fut.as_mut().poll(&mut cx) {
// 			Poll::Ready(result) => return result,
// 			Poll::Pending => {
// 				// For pure computation, this should never happen
// 				panic!("Function execution requires async operations which are not supported");
// 			}
// 		}
// 	}
// }
//
// /// Execute a function once (for no-arg functions) and return the result.
// fn execute_function_once(program: &crate::bytecode::Program, func_index: u16, _args: &Columns) ->
// Result<OperandValue> { 	use crate::vmcore::{VmContext, VmState};
//
// 	let func_def = &program.functions[func_index as usize];
//
// 	// Create a dummy source registry for pure function execution
// 	let sources: Arc<dyn crate::source::SourceRegistry> = Arc::new(EmptySourceRegistry);
// 	let context = Arc::new(VmContext::new(sources));
//
// 	// Create a mini VM to execute just this function
// 	let mut vm = VmState::new(Arc::new(program.clone()), context);
//
// 	// Set IP to function body
// 	vm.ip = func_def.bytecode_offset;
//
// 	// Execute until we hit a Return or reach end of function
// 	let end_offset = func_def.bytecode_offset + func_def.bytecode_len;
//
// 	// Push a sentinel call frame so Return knows where to stop
// 	// scope_depth = 1 because the VM starts with global scope
// 	let frame = CallFrame::new(func_index, end_offset, 0, 0, 1);
// 	vm.call_stack.push(frame);
//
// 	// Execute instructions until return
// 	loop {
// 		if vm.ip >= end_offset {
// 			break;
// 		}
//
// 		match block_on_simple(vm.step(rx))? {
// 			DispatchResult::Halt => break,
// 			DispatchResult::Continue => {}
// 			DispatchResult::Yield(_) => break,
// 		}
//
// 		if vm.call_stack.is_empty() {
// 			break;
// 		}
// 	}
//
// 	// Get result from operand stack
// 	vm.pop_operand()
// }
//
// /// Execute a function with columnar arguments (row-by-row execution).
// fn execute_function_columnar(
// 	program: &crate::bytecode::Program,
// 	func_index: u16,
// 	ctx: VmFunctionContext,
// ) -> Result<ColumnData> {
// 	use crate::vmcore::{VmContext, VmState};
//
// 	let func_def = &program.functions[func_index as usize];
// 	let row_count = ctx.row_count;
//
// 	if row_count == 0 {
// 		return Ok(ColumnData::int8(Vec::new()));
// 	}
//
// 	// Create a dummy source registry for pure function execution
// 	let sources: Arc<dyn crate::source::SourceRegistry> = Arc::new(EmptySourceRegistry);
// 	let context = Arc::new(VmContext::new(sources));
//
// 	// Execute the function for each row and collect results
// 	let mut results: Vec<reifydb_type::Value> = Vec::with_capacity(row_count);
//
// 	for row_idx in 0..row_count {
// 		let mut vm = VmState::new(Arc::new(program.clone()), context.clone());
//
// 		// Enter scope and bind parameters with scalar values from this row
// 		vm.scopes.push();
// 		for (param_idx, param) in func_def.parameters.iter().enumerate() {
// 			if param_idx < ctx.columns.len() {
// 				let col = &ctx.columns[param_idx];
// 				let scalar_value = col.data().get_value(row_idx);
// 				vm.scopes.set(param.name.clone(), OperandValue::Scalar(scalar_value));
// 			}
// 		}
//
// 		vm.ip = func_def.bytecode_offset;
// 		let end_offset = func_def.bytecode_offset + func_def.bytecode_len;
//
// 		let frame = CallFrame::new(func_index, end_offset, 0, 0, 1);
// 		vm.call_stack.push(frame);
//
// 		loop {
// 			if vm.ip >= end_offset {
// 				break;
// 			}
//
// 			match block_on_simple(vm.step(rx))? {
// 				DispatchResult::Halt => break,
// 				DispatchResult::Continue => {}
// 				DispatchResult::Yield(_) => break,
// 			}
//
// 			if vm.call_stack.is_empty() {
// 				break;
// 			}
// 		}
//
// 		let result = vm.pop_operand().unwrap_or(OperandValue::Scalar(reifydb_type::Value::Undefined));
// 		match result {
// 			OperandValue::Scalar(v) => results.push(v),
// 			_ => results.push(reifydb_type::Value::Undefined),
// 		}
// 	}
//
// 	// Convert results to column data
// 	let first_typed = results.iter().find(|v| !matches!(v, reifydb_type::Value::Undefined));
//
// 	match first_typed {
// 		Some(reifydb_type::Value::Boolean(_)) => {
// 			let bools: Vec<bool> = results
// 				.into_iter()
// 				.map(|v| match v {
// 					reifydb_type::Value::Boolean(b) => b,
// 					_ => false,
// 				})
// 				.collect();
// 			Ok(ColumnData::bool(bools))
// 		}
// 		Some(reifydb_type::Value::Int8(_)) | None => {
// 			let ints: Vec<i64> = results
// 				.into_iter()
// 				.map(|v| match v {
// 					reifydb_type::Value::Int8(n) => n,
// 					_ => 0,
// 				})
// 				.collect();
// 			Ok(ColumnData::int8(ints))
// 		}
// 		Some(reifydb_type::Value::Float8(_)) => {
// 			let floats: Vec<f64> = results
// 				.into_iter()
// 				.map(|v| match v {
// 					reifydb_type::Value::Float8(f) => f.value(),
// 					_ => 0.0,
// 				})
// 				.collect();
// 			Ok(ColumnData::float8(floats))
// 		}
// 		Some(reifydb_type::Value::Utf8(_)) => {
// 			let strings: Vec<String> = results
// 				.into_iter()
// 				.map(|v| match v {
// 					reifydb_type::Value::Utf8(s) => s,
// 					_ => String::new(),
// 				})
// 				.collect();
// 			Ok(ColumnData::utf8(strings))
// 		}
// 		_ => {
// 			let ints: Vec<i64> = results
// 				.into_iter()
// 				.map(|v| match v {
// 					reifydb_type::Value::Int8(n) => n,
// 					_ => 0,
// 				})
// 				.collect();
// 			Ok(ColumnData::int8(ints))
// 		}
// 	}
// }
//
// /// Empty source registry for pure function execution (no table access).
// struct EmptySourceRegistry;
//
// impl crate::source::SourceRegistry for EmptySourceRegistry {
// 	fn get_source(&self, _name: &str) -> Option<Box<dyn crate::source::TableSource>> {
// 		None
// 	}
// }
