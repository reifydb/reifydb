// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Bytecode interpreter.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::{ColumnData, Columns};

use super::{
	call_stack::CallFrame,
	state::{OperandValue, VmState},
};
use crate::{
	bytecode::{BytecodeReader, Opcode, OperatorKind},
	error::{Result, VmError},
	expr::{EvalContext, EvalValue, VmFunctionContext, VmFunctionExecutor},
	operator::{FilterOp, ProjectOp, SelectOp, SortOp, TakeOp},
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

/// Decoded instruction with all operands.
#[derive(Debug, Clone)]
enum DecodedInstruction {
	PushConst {
		index: u16,
	},
	PushExpr {
		index: u16,
	},
	PushColRef {
		name_index: u16,
	},
	PushColList {
		index: u16,
	},
	PushSortSpec {
		index: u16,
	},
	PushExtSpec {
		index: u16,
	},
	Pop,
	Dup,
	LoadVar {
		name_index: u16,
	},
	StoreVar {
		name_index: u16,
	},
	StorePipeline {
		name_index: u16,
	},
	LoadPipeline {
		name_index: u16,
	},
	UpdateVar {
		name_index: u16,
	},
	Source {
		source_index: u16,
	},
	Inline,
	Apply {
		op_kind: OperatorKind,
	},
	Collect,
	PopPipeline,
	Merge,
	DupPipeline,
	Jump {
		offset: i16,
	},
	JumpIf {
		offset: i16,
	},
	JumpIfNot {
		offset: i16,
	},
	Call {
		func_index: u16,
	},
	Return,
	CallBuiltin {
		_builtin_id: u16,
		_arg_count: u8,
	},
	EnterScope,
	ExitScope,
	FrameLen,
	FrameRow,
	GetField {
		name_index: u16,
	},
	IntAdd,
	IntLt,
	IntEq,
	IntSub,
	IntMul,
	IntDiv,
	// Columnar operations
	ColAdd,
	ColSub,
	ColMul,
	ColDiv,
	ColLt,
	ColLe,
	ColGt,
	ColGe,
	ColEq,
	ColNe,
	ColAnd,
	ColOr,
	ColNot,
	PrintOut,
	Nop,
	Halt,
}

impl VmState {
	/// Execute the program until halt or yield.
	pub async fn execute(&mut self) -> Result<Option<Pipeline>> {
		loop {
			let result = self.step().await?;
			match result {
				DispatchResult::Continue => continue,
				DispatchResult::Halt => break,
				DispatchResult::Yield(pipeline) => return Ok(Some(pipeline)),
			}
		}

		// Return top of pipeline stack if present
		Ok(self.pipeline_stack.pop())
	}

	/// Decode the instruction at the current IP.
	fn decode(&self) -> Result<(DecodedInstruction, usize)> {
		let mut reader = BytecodeReader::new(&self.program.bytecode);
		reader.set_position(self.ip);

		let opcode = reader.read_opcode().ok_or(VmError::InvalidBytecode {
			position: self.ip,
		})?;

		let instruction = match opcode {
			Opcode::PushConst => {
				let index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::PushConst {
					index,
				}
			}
			Opcode::PushExpr => {
				let index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::PushExpr {
					index,
				}
			}
			Opcode::PushColRef => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::PushColRef {
					name_index,
				}
			}
			Opcode::PushColList => {
				let index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::PushColList {
					index,
				}
			}
			Opcode::PushSortSpec => {
				let index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::PushSortSpec {
					index,
				}
			}
			Opcode::PushExtSpec => {
				let index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::PushExtSpec {
					index,
				}
			}
			Opcode::Pop => DecodedInstruction::Pop,
			Opcode::Dup => DecodedInstruction::Dup,
			Opcode::LoadVar => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::LoadVar {
					name_index,
				}
			}
			Opcode::StoreVar => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::StoreVar {
					name_index,
				}
			}
			Opcode::StorePipeline => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::StorePipeline {
					name_index,
				}
			}
			Opcode::LoadPipeline => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::LoadPipeline {
					name_index,
				}
			}
			Opcode::UpdateVar => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::UpdateVar {
					name_index,
				}
			}
			Opcode::Source => {
				let source_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::Source {
					source_index,
				}
			}
			Opcode::Inline => DecodedInstruction::Inline,
			Opcode::Apply => {
				let op_kind_byte = reader.read_u8().ok_or(VmError::UnexpectedEndOfBytecode)?;
				let op_kind = OperatorKind::try_from(op_kind_byte).map_err(|_| {
					VmError::UnknownOperatorKind {
						kind: op_kind_byte,
					}
				})?;
				DecodedInstruction::Apply {
					op_kind,
				}
			}
			Opcode::Collect => DecodedInstruction::Collect,
			Opcode::PopPipeline => DecodedInstruction::PopPipeline,
			Opcode::Merge => DecodedInstruction::Merge,
			Opcode::DupPipeline => DecodedInstruction::DupPipeline,
			Opcode::Jump => {
				let offset = reader.read_i16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::Jump {
					offset,
				}
			}
			Opcode::JumpIf => {
				let offset = reader.read_i16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::JumpIf {
					offset,
				}
			}
			Opcode::JumpIfNot => {
				let offset = reader.read_i16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::JumpIfNot {
					offset,
				}
			}
			Opcode::Call => {
				let func_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::Call {
					func_index,
				}
			}
			Opcode::Return => DecodedInstruction::Return,
			Opcode::CallBuiltin => {
				let builtin_id = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				let arg_count = reader.read_u8().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::CallBuiltin {
					_builtin_id: builtin_id,
					_arg_count: arg_count,
				}
			}
			Opcode::EnterScope => DecodedInstruction::EnterScope,
			Opcode::ExitScope => DecodedInstruction::ExitScope,
			Opcode::FrameLen => DecodedInstruction::FrameLen,
			Opcode::FrameRow => DecodedInstruction::FrameRow,
			Opcode::GetField => {
				let name_index = reader.read_u16().ok_or(VmError::UnexpectedEndOfBytecode)?;
				DecodedInstruction::GetField {
					name_index,
				}
			}
			Opcode::IntAdd => DecodedInstruction::IntAdd,
			Opcode::IntLt => DecodedInstruction::IntLt,
			Opcode::IntEq => DecodedInstruction::IntEq,
			Opcode::IntSub => DecodedInstruction::IntSub,
			Opcode::IntMul => DecodedInstruction::IntMul,
			Opcode::IntDiv => DecodedInstruction::IntDiv,
			// Columnar operations
			Opcode::ColAdd => DecodedInstruction::ColAdd,
			Opcode::ColSub => DecodedInstruction::ColSub,
			Opcode::ColMul => DecodedInstruction::ColMul,
			Opcode::ColDiv => DecodedInstruction::ColDiv,
			Opcode::ColLt => DecodedInstruction::ColLt,
			Opcode::ColLe => DecodedInstruction::ColLe,
			Opcode::ColGt => DecodedInstruction::ColGt,
			Opcode::ColGe => DecodedInstruction::ColGe,
			Opcode::ColEq => DecodedInstruction::ColEq,
			Opcode::ColNe => DecodedInstruction::ColNe,
			Opcode::ColAnd => DecodedInstruction::ColAnd,
			Opcode::ColOr => DecodedInstruction::ColOr,
			Opcode::ColNot => DecodedInstruction::ColNot,
			Opcode::PrintOut => DecodedInstruction::PrintOut,
			Opcode::Nop => DecodedInstruction::Nop,
			Opcode::Halt => DecodedInstruction::Halt,
		};

		Ok((instruction, reader.position()))
	}

	/// Execute a single instruction.
	pub async fn step(&mut self) -> Result<DispatchResult> {
		let (instruction, next_ip) = self.decode()?;

		match instruction {
			// ─────────────────────────────────────────────────────────
			// Stack Operations
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::PushConst {
				index,
			} => {
				let value = self.program.constants.get(index as usize).cloned().ok_or(
					VmError::InvalidConstantIndex {
						index,
					},
				)?;
				self.push_operand(OperandValue::Scalar(value))?;
				self.ip = next_ip;
			}

			DecodedInstruction::PushExpr {
				index,
			} => {
				self.push_operand(OperandValue::ExprRef(index))?;
				self.ip = next_ip;
			}

			DecodedInstruction::PushColRef {
				name_index,
			} => {
				let name = self.get_constant_string(name_index)?;
				self.push_operand(OperandValue::ColRef(name))?;
				self.ip = next_ip;
			}

			DecodedInstruction::PushColList {
				index,
			} => {
				let columns = self.program.column_lists.get(index as usize).cloned().ok_or(
					VmError::InvalidColumnListIndex {
						index,
					},
				)?;
				self.push_operand(OperandValue::ColList(columns))?;
				self.ip = next_ip;
			}

			DecodedInstruction::PushSortSpec {
				index,
			} => {
				self.push_operand(OperandValue::SortSpecRef(index))?;
				self.ip = next_ip;
			}

			DecodedInstruction::PushExtSpec {
				index,
			} => {
				self.push_operand(OperandValue::ExtSpecRef(index))?;
				self.ip = next_ip;
			}

			DecodedInstruction::Pop => {
				self.pop_operand()?;
				self.ip = next_ip;
			}

			DecodedInstruction::Dup => {
				let value = self.peek_operand()?.clone();
				self.push_operand(value)?;
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Variable Operations
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::LoadVar {
				name_index,
			} => {
				let name = self.get_constant_string(name_index)?;
				let value = self.scopes.get(&name).cloned().ok_or(VmError::UndefinedVariable {
					name,
				})?;
				self.push_operand(value)?;
				self.ip = next_ip;
			}

			DecodedInstruction::StoreVar {
				name_index,
			} => {
				let name = self.get_constant_string(name_index)?;
				let value = self.pop_operand()?;
				self.scopes.set(name, value);
				self.ip = next_ip;
			}

			DecodedInstruction::StorePipeline {
				name_index,
			} => {
				let name = self.get_constant_string(name_index)?;
				let pipeline = self.pop_pipeline()?;
				let handle = self.register_pipeline(pipeline);
				self.scopes.set(name, OperandValue::PipelineRef(handle));
				self.ip = next_ip;
			}

			DecodedInstruction::LoadPipeline {
				name_index,
			} => {
				let name = self.get_constant_string(name_index)?;
				let value = self.scopes.get(&name).cloned().ok_or(VmError::UndefinedVariable {
					name: name.clone(),
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

			DecodedInstruction::UpdateVar {
				name_index,
			} => {
				let name = self.get_constant_string(name_index)?;
				let value = self.pop_operand()?;
				// Update existing variable (searches all scopes)
				if !self.scopes.update(&name, value) {
					return Err(VmError::UndefinedVariable {
						name,
					});
				}
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Pipeline Operations
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::Source {
				source_index,
			} => {
				let source_def = self.program.sources.get(source_index as usize).ok_or(
					VmError::InvalidSourceIndex {
						index: source_index,
					},
				)?;
				let source = self.context.sources.get_source(&source_def.name).ok_or(
					VmError::TableNotFound {
						name: source_def.name.clone(),
					},
				)?;
				let pipeline = source.scan();
				self.push_pipeline(pipeline)?;
				self.ip = next_ip;
			}

			DecodedInstruction::Inline => {
				let pipeline: Pipeline = Box::pin(futures_util::stream::empty());
				self.push_pipeline(pipeline)?;
				self.ip = next_ip;
			}

			DecodedInstruction::Apply {
				op_kind,
			} => {
				self.apply_operator(op_kind)?;
				self.ip = next_ip;
			}

			DecodedInstruction::Collect => {
				let pipeline = self.pop_pipeline()?;
				let columns = crate::pipeline::collect(pipeline).await?;
				self.push_operand(OperandValue::Frame(columns))?;
				self.ip = next_ip;
			}

			DecodedInstruction::PopPipeline => {
				let _ = self.pop_pipeline()?;
				self.ip = next_ip;
			}

			DecodedInstruction::Merge | DecodedInstruction::DupPipeline => {
				return Err(VmError::UnsupportedOperation {
					operation: "Merge/DupPipeline".to_string(),
				});
			}

			// ─────────────────────────────────────────────────────────
			// Control Flow
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::Jump {
				offset,
			} => {
				let new_ip = (next_ip as i32 + offset as i32) as usize;
				self.ip = new_ip;
			}

			DecodedInstruction::JumpIf {
				offset,
			} => {
				let value = self.pop_operand()?;
				if self.is_truthy(&value)? {
					let new_ip = (next_ip as i32 + offset as i32) as usize;
					self.ip = new_ip;
				} else {
					self.ip = next_ip;
				}
			}

			DecodedInstruction::JumpIfNot {
				offset,
			} => {
				let value = self.pop_operand()?;
				if !self.is_truthy(&value)? {
					let new_ip = (next_ip as i32 + offset as i32) as usize;
					self.ip = new_ip;
				} else {
					self.ip = next_ip;
				}
			}

			// ─────────────────────────────────────────────────────────
			// Function Calls
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::Call {
				func_index,
			} => {
				let func_def = self
					.program
					.functions
					.get(func_index as usize)
					.ok_or(VmError::InvalidFunctionIndex {
						index: func_index,
					})?
					.clone();

				// Push call frame
				let frame = CallFrame::new(
					func_index,
					next_ip,
					self.operand_stack.len().saturating_sub(func_def.parameters.len()),
					self.pipeline_stack.len(),
					self.scopes.depth(),
				);

				if !self.call_stack.push(frame) {
					return Err(VmError::StackOverflow {
						stack: "call".into(),
					});
				}

				// Enter new scope and bind parameters
				self.scopes.push();
				for (i, param) in func_def.parameters.iter().enumerate() {
					let arg_index = self.operand_stack.len() - func_def.parameters.len() + i;
					if let Some(value) = self.operand_stack.get(arg_index).cloned() {
						self.scopes.set(param.name.clone(), value);
					}
				}

				// Jump to function body
				self.ip = func_def.bytecode_offset;
			}

			DecodedInstruction::Return => {
				let frame = self.call_stack.pop().ok_or(VmError::ReturnOutsideFunction)?;

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

			DecodedInstruction::CallBuiltin {
				..
			} => {
				return Err(VmError::UnsupportedOperation {
					operation: "CallBuiltin".to_string(),
				});
			}

			// ─────────────────────────────────────────────────────────
			// Scope Management
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::EnterScope => {
				self.scopes.push();
				self.ip = next_ip;
			}

			DecodedInstruction::ExitScope => {
				self.scopes.pop();
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Frame/Record Operations
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::FrameLen => {
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

			DecodedInstruction::FrameRow => {
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

			DecodedInstruction::GetField {
				name_index,
			} => {
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
			DecodedInstruction::IntAdd => {
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

			DecodedInstruction::IntLt => {
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

			DecodedInstruction::IntEq => {
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

			DecodedInstruction::IntSub => {
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

			DecodedInstruction::IntMul => {
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

			DecodedInstruction::IntDiv => {
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

			// ─────────────────────────────────────────────────────────
			// Columnar Operations
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::ColAdd
			| DecodedInstruction::ColSub
			| DecodedInstruction::ColMul
			| DecodedInstruction::ColDiv
			| DecodedInstruction::ColLt
			| DecodedInstruction::ColLe
			| DecodedInstruction::ColGt
			| DecodedInstruction::ColGe
			| DecodedInstruction::ColEq
			| DecodedInstruction::ColNe
			| DecodedInstruction::ColAnd
			| DecodedInstruction::ColOr => {
				return Err(VmError::UnsupportedOperation {
					operation: "columnar binary operation (not yet implemented)".to_string(),
				});
			}

			DecodedInstruction::ColNot => {
				return Err(VmError::UnsupportedOperation {
					operation: "columnar NOT operation (not yet implemented)".to_string(),
				});
			}

			// ─────────────────────────────────────────────────────────
			// I/O Operations
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::PrintOut => {
				let value = self.pop_operand()?;
				self.print_value(&value);
				self.ip = next_ip;
			}

			// ─────────────────────────────────────────────────────────
			// Control
			// ─────────────────────────────────────────────────────────
			DecodedInstruction::Nop => {
				self.ip = next_ip;
			}

			DecodedInstruction::Halt => {
				return Ok(DispatchResult::Halt);
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
		};

		self.push_pipeline(new_pipeline)?;
		Ok(())
	}

	/// Capture all scope variables into an EvalContext for expression evaluation.
	fn capture_scope_context(&self) -> EvalContext {
		let mut variables = HashMap::new();

		// Iterate through all scope levels and capture variables
		for scope in self.scopes.iter() {
			for (name, value) in scope.iter() {
				// Only add if not already present (inner scopes shadow outer)
				if !variables.contains_key(name) {
					if let Some(eval_value) = operand_to_eval_value(value) {
						variables.insert(name.clone(), eval_value);
					}
				}
			}
		}

		// Build function executor from program's functions
		let functions = self.build_function_executor();

		EvalContext {
			variables,
			subquery_executor: self.context.subquery_executor.clone(),
			current_row_values: None,
			functions: Some(functions),
		}
	}

	/// Build a VmFunctionExecutor from the program's bytecode functions.
	///
	/// Each bytecode function is wrapped to execute in a fresh VM context.
	/// For functions that return scalar values, the result is broadcast to match row_count.
	fn build_function_executor(&self) -> VmFunctionExecutor {
		let mut executor = VmFunctionExecutor::new();

		for func_def in &self.program.functions {
			let func_name = func_def.name.clone();
			let program = self.program.clone();
			let func_index =
				self.program.functions.iter().position(|f| f.name == func_name).unwrap() as u16;

			// Create a wrapper that executes the bytecode function
			let wrapper: crate::expr::VmScalarFn =
				Arc::new(move |ctx: VmFunctionContext| -> Result<ColumnData> {
					let func_def = &program.functions[func_index as usize];

					// For functions with column arguments, we need columnar execution.
					// For now, handle the simple case of no-arg functions that return constants.
					if !func_def.parameters.is_empty() && !ctx.columns.is_empty() {
						// TODO: Implement full columnar function execution
						// For now, execute row-by-row for functions with arguments
						return execute_function_columnar(&program, func_index, ctx);
					}

					// Execute the function in a fresh VM state
					let result = execute_function_once(&program, func_index, ctx.columns)?;

					// Broadcast scalar result to column
					match result {
						OperandValue::Scalar(v) => {
							Ok(broadcast_scalar_to_column(&v, ctx.row_count))
						}
						OperandValue::Column(col) => Ok(col.data().clone()),
						_ => Err(VmError::UnsupportedOperation {
							operation: format!(
								"function returned {:?}, expected scalar or column",
								result
							),
						}),
					}
				});

			executor.register(func_name, wrapper);
		}

		executor
	}

	/// Print a value to stdout (for console::log).
	fn print_value(&self, value: &OperandValue) {
		match value {
			OperandValue::Scalar(v) => match v {
				reifydb_type::Value::Undefined => println!("undefined"),
				reifydb_type::Value::Boolean(b) => println!("{}", b),
				reifydb_type::Value::Int8(n) => println!("{}", n),
				reifydb_type::Value::Float8(f) => println!("{}", f),
				reifydb_type::Value::Utf8(s) => println!("{}", s),
				_ => println!("{:?}", v),
			},
			OperandValue::Record(r) => {
				print!("{{ ");
				for (i, (name, val)) in r.fields.iter().enumerate() {
					if i > 0 {
						print!(", ");
					}
					print!("{}: {:?}", name, val);
				}
				println!(" }}");
			}
			OperandValue::Frame(cols) => {
				println!("Frame({} columns, {} rows)", cols.len(), cols.row_count());
			}
			_ => println!("{:?}", value),
		}
	}
}

/// Convert an OperandValue to an EvalValue if possible.
fn operand_to_eval_value(value: &OperandValue) -> Option<EvalValue> {
	match value {
		OperandValue::Scalar(v) => Some(EvalValue::Scalar(v.clone())),
		OperandValue::Record(r) => Some(EvalValue::Record(r.clone())),
		_ => None, // Other types cannot be used in expressions
	}
}

/// Broadcast a scalar value to a column with the given row count.
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

/// Simple blocking executor for futures that complete immediately.
fn block_on_simple<F: std::future::Future>(fut: F) -> F::Output {
	use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

	fn noop_clone(_: *const ()) -> RawWaker {
		noop_raw_waker()
	}
	fn noop(_: *const ()) {}
	fn noop_raw_waker() -> RawWaker {
		static VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
		RawWaker::new(std::ptr::null(), &VTABLE)
	}

	let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
	let mut cx = Context::from_waker(&waker);
	let mut fut = std::pin::pin!(fut);

	loop {
		match fut.as_mut().poll(&mut cx) {
			Poll::Ready(result) => return result,
			Poll::Pending => {
				// For pure computation, this should never happen
				panic!("Function execution requires async operations which are not supported");
			}
		}
	}
}

/// Execute a function once (for no-arg functions) and return the result.
fn execute_function_once(program: &crate::bytecode::Program, func_index: u16, _args: &Columns) -> Result<OperandValue> {
	use crate::vmcore::{VmContext, VmState};

	let func_def = &program.functions[func_index as usize];

	// Create a dummy source registry for pure function execution
	let sources: Arc<dyn crate::source::SourceRegistry> = Arc::new(EmptySourceRegistry);
	let context = Arc::new(VmContext::new(sources));

	// Create a mini VM to execute just this function
	let mut vm = VmState::new(Arc::new(program.clone()), context);

	// Set IP to function body
	vm.ip = func_def.bytecode_offset;

	// Execute until we hit a Return or reach end of function
	let end_offset = func_def.bytecode_offset + func_def.bytecode_len;

	// Push a sentinel call frame so Return knows where to stop
	// scope_depth = 1 because the VM starts with global scope
	let frame = CallFrame::new(func_index, end_offset, 0, 0, 1);
	vm.call_stack.push(frame);

	// Execute instructions until return
	loop {
		if vm.ip >= end_offset {
			break;
		}

		match block_on_simple(vm.step())? {
			DispatchResult::Halt => break,
			DispatchResult::Continue => {}
			DispatchResult::Yield(_) => break,
		}

		if vm.call_stack.is_empty() {
			break;
		}
	}

	// Get result from operand stack
	vm.pop_operand()
}

/// Execute a function with columnar arguments (row-by-row execution).
fn execute_function_columnar(
	program: &crate::bytecode::Program,
	func_index: u16,
	ctx: VmFunctionContext,
) -> Result<ColumnData> {
	use crate::vmcore::{VmContext, VmState};

	let func_def = &program.functions[func_index as usize];
	let row_count = ctx.row_count;

	if row_count == 0 {
		return Ok(ColumnData::int8(Vec::new()));
	}

	// Create a dummy source registry for pure function execution
	let sources: Arc<dyn crate::source::SourceRegistry> = Arc::new(EmptySourceRegistry);
	let context = Arc::new(VmContext::new(sources));

	// Execute the function for each row and collect results
	let mut results: Vec<reifydb_type::Value> = Vec::with_capacity(row_count);

	for row_idx in 0..row_count {
		let mut vm = VmState::new(Arc::new(program.clone()), context.clone());

		// Enter scope and bind parameters with scalar values from this row
		vm.scopes.push();
		for (param_idx, param) in func_def.parameters.iter().enumerate() {
			if param_idx < ctx.columns.len() {
				let col = &ctx.columns[param_idx];
				let scalar_value = col.data().get_value(row_idx);
				vm.scopes.set(param.name.clone(), OperandValue::Scalar(scalar_value));
			}
		}

		vm.ip = func_def.bytecode_offset;
		let end_offset = func_def.bytecode_offset + func_def.bytecode_len;

		let frame = CallFrame::new(func_index, end_offset, 0, 0, 1);
		vm.call_stack.push(frame);

		loop {
			if vm.ip >= end_offset {
				break;
			}

			match block_on_simple(vm.step())? {
				DispatchResult::Halt => break,
				DispatchResult::Continue => {}
				DispatchResult::Yield(_) => break,
			}

			if vm.call_stack.is_empty() {
				break;
			}
		}

		let result = vm.pop_operand().unwrap_or(OperandValue::Scalar(reifydb_type::Value::Undefined));
		match result {
			OperandValue::Scalar(v) => results.push(v),
			_ => results.push(reifydb_type::Value::Undefined),
		}
	}

	// Convert results to column data
	let first_typed = results.iter().find(|v| !matches!(v, reifydb_type::Value::Undefined));

	match first_typed {
		Some(reifydb_type::Value::Boolean(_)) => {
			let bools: Vec<bool> = results
				.into_iter()
				.map(|v| match v {
					reifydb_type::Value::Boolean(b) => b,
					_ => false,
				})
				.collect();
			Ok(ColumnData::bool(bools))
		}
		Some(reifydb_type::Value::Int8(_)) | None => {
			let ints: Vec<i64> = results
				.into_iter()
				.map(|v| match v {
					reifydb_type::Value::Int8(n) => n,
					_ => 0,
				})
				.collect();
			Ok(ColumnData::int8(ints))
		}
		Some(reifydb_type::Value::Float8(_)) => {
			let floats: Vec<f64> = results
				.into_iter()
				.map(|v| match v {
					reifydb_type::Value::Float8(f) => f.value(),
					_ => 0.0,
				})
				.collect();
			Ok(ColumnData::float8(floats))
		}
		Some(reifydb_type::Value::Utf8(_)) => {
			let strings: Vec<String> = results
				.into_iter()
				.map(|v| match v {
					reifydb_type::Value::Utf8(s) => s,
					_ => String::new(),
				})
				.collect();
			Ok(ColumnData::utf8(strings))
		}
		_ => {
			let ints: Vec<i64> = results
				.into_iter()
				.map(|v| match v {
					reifydb_type::Value::Int8(n) => n,
					_ => 0,
				})
				.collect();
			Ok(ColumnData::int8(ints))
		}
	}
}

/// Empty source registry for pure function execution (no table access).
struct EmptySourceRegistry;

impl crate::source::SourceRegistry for EmptySourceRegistry {
	fn get_source(&self, _name: &str) -> Option<Box<dyn crate::source::TableSource>> {
		None
	}
}
