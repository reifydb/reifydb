// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Text formatting for VM trace output.

use std::fmt::{self, Display, Write};

use super::entry::{
	CallFrameSnapshot, FrameSnapshot, InstructionSnapshot, OperandSnapshot, RecordSnapshot, ScopeSnapshot,
	StateChange, StateSnapshot, TraceEntry,
};

const SEPARATOR: &str = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";

impl Display for TraceEntry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// Header
		writeln!(f, "━━━ Step {} {}", self.step, SEPARATOR)?;

		// IP
		writeln!(f, "  IP: 0x{:02X} → 0x{:02X}", self.ip_before, self.ip_after)?;

		// Bytecode
		write!(f, "  Bytecode:")?;
		for byte in &self.bytecode {
			write!(f, " {:02X}", byte)?;
		}
		writeln!(f)?;

		// Instruction
		writeln!(f, "  Instruction: {}", self.instruction)?;

		// Changes
		writeln!(f)?;
		writeln!(f, "  Changes:")?;
		if self.changes.is_empty() {
			writeln!(f, "    (none)")?;
		} else {
			for change in &self.changes {
				writeln!(f, "    {}", change)?;
			}
		}

		// State After
		writeln!(f)?;
		writeln!(f, "  State After:")?;
		write!(f, "{}", IndentedState(&self.state))?;

		// Result
		writeln!(f)?;
		writeln!(f, "  Result: {}", self.result)?;

		Ok(())
	}
}

/// Helper to format state with indentation.
struct IndentedState<'a>(&'a StateSnapshot);

impl Display for IndentedState<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let state = self.0;

		// Operand Stack
		write!(f, "    Operand Stack: ")?;
		if state.operand_stack.is_empty() {
			writeln!(f, "(empty)")?;
		} else {
			write!(f, "[")?;
			for (i, op) in state.operand_stack.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "{}", op)?;
			}
			writeln!(f, "]")?;
		}

		// Pipeline Stack
		write!(f, "    Pipeline Stack: ")?;
		if state.pipeline_stack.is_empty() {
			writeln!(f, "(empty)")?;
		} else {
			write!(f, "[")?;
			for (i, p) in state.pipeline_stack.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "{}", p)?;
			}
			writeln!(f, "]")?;
		}

		// Scopes
		write!(f, "    Scopes: ")?;
		format_scopes(f, &state.scopes)?;
		writeln!(f)?;

		// Call Stack
		write!(f, "    Call Stack: ")?;
		if state.call_stack.is_empty() {
			writeln!(f, "(empty)")?;
		} else {
			writeln!(f, "[")?;
			for frame in &state.call_stack {
				writeln!(f, "      {}", frame)?;
			}
			write!(f, "    ]")?;
		}

		Ok(())
	}
}

fn format_scopes(f: &mut fmt::Formatter<'_>, scopes: &[ScopeSnapshot]) -> fmt::Result {
	write!(f, "{{ ")?;
	for (i, scope) in scopes.iter().enumerate() {
		if i > 0 {
			write!(f, ", ")?;
		}
		let scope_name = if scope.depth == 0 {
			"global"
		} else {
			&format!("scope{}", scope.depth)
		};
		write!(f, "{}: {{ ", scope_name)?;
		for (j, (name, value)) in scope.variables.iter().enumerate() {
			if j > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{}: {}", name, value)?;
		}
		write!(f, " }}")?;
	}
	write!(f, " }}")?;
	Ok(())
}

impl Display for InstructionSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			InstructionSnapshot::PushConst {
				index,
				value,
			} => {
				write!(f, "PushConst index={} value={:?}", index, value)
			}
			InstructionSnapshot::PushExpr {
				index,
			} => write!(f, "PushExpr index={}", index),
			InstructionSnapshot::PushColRef {
				name,
			} => write!(f, "PushColRef name=\"{}\"", name),
			InstructionSnapshot::PushColList {
				columns,
			} => {
				write!(f, "PushColList columns={:?}", columns)
			}
			InstructionSnapshot::PushSortSpec {
				index,
			} => write!(f, "PushSortSpec index={}", index),
			InstructionSnapshot::PushExtSpec {
				index,
			} => write!(f, "PushExtSpec index={}", index),
			InstructionSnapshot::LoadVar {
				name,
			} => write!(f, "LoadVar name=\"{}\"", name),
			InstructionSnapshot::StoreVar {
				name,
			} => write!(f, "StoreVar name=\"{}\"", name),
			InstructionSnapshot::Source {
				index,
				name,
			} => {
				write!(f, "Source index={} name=\"{}\"", index, name)
			}
			InstructionSnapshot::Inline => write!(f, "Inline"),
			InstructionSnapshot::Apply {
				operator,
			} => write!(f, "Apply operator={}", operator),
			InstructionSnapshot::Collect => write!(f, "Collect"),
			InstructionSnapshot::PopPipeline => write!(f, "PopPipeline"),
			InstructionSnapshot::Merge => write!(f, "Merge"),
			InstructionSnapshot::Jump {
				offset,
				target,
			} => {
				write!(f, "Jump offset={} target=0x{:02X}", offset, target)
			}
			InstructionSnapshot::JumpIf {
				offset,
				target,
			} => {
				write!(f, "JumpIf offset={} target=0x{:02X}", offset, target)
			}
			InstructionSnapshot::JumpIfNot {
				offset,
				target,
			} => {
				write!(f, "JumpIfNot offset={} target=0x{:02X}", offset, target)
			}
			InstructionSnapshot::Call {
				func_index,
			} => write!(f, "Call func_index={}", func_index),
			InstructionSnapshot::Return => write!(f, "Return"),
			InstructionSnapshot::CallBuiltin {
				builtin_id,
				arg_count,
			} => {
				write!(f, "CallBuiltin builtin_id={} arg_count={}", builtin_id, arg_count)
			}
			InstructionSnapshot::EnterScope => write!(f, "EnterScope"),
			InstructionSnapshot::ExitScope => write!(f, "ExitScope"),
			InstructionSnapshot::FrameLen => write!(f, "FrameLen"),
			InstructionSnapshot::FrameRow => write!(f, "FrameRow"),
			InstructionSnapshot::GetField {
				name,
			} => write!(f, "GetField name=\"{}\"", name),
			InstructionSnapshot::IntAdd => write!(f, "IntAdd"),
			InstructionSnapshot::IntLt => write!(f, "IntLt"),
			InstructionSnapshot::IntEq => write!(f, "IntEq"),
			InstructionSnapshot::IntSub => write!(f, "IntSub"),
			InstructionSnapshot::IntMul => write!(f, "IntMul"),
			InstructionSnapshot::IntDiv => write!(f, "IntDiv"),
			InstructionSnapshot::ColAdd => write!(f, "ColAdd"),
			InstructionSnapshot::ColSub => write!(f, "ColSub"),
			InstructionSnapshot::ColMul => write!(f, "ColMul"),
			InstructionSnapshot::ColDiv => write!(f, "ColDiv"),
			InstructionSnapshot::ColLt => write!(f, "ColLt"),
			InstructionSnapshot::ColLe => write!(f, "ColLe"),
			InstructionSnapshot::ColGt => write!(f, "ColGt"),
			InstructionSnapshot::ColGe => write!(f, "ColGe"),
			InstructionSnapshot::ColEq => write!(f, "ColEq"),
			InstructionSnapshot::ColNe => write!(f, "ColNe"),
			InstructionSnapshot::ColAnd => write!(f, "ColAnd"),
			InstructionSnapshot::ColOr => write!(f, "ColOr"),
			InstructionSnapshot::ColNot => write!(f, "ColNot"),
			InstructionSnapshot::PrintOut => write!(f, "PrintOut"),
			InstructionSnapshot::Nop => write!(f, "Nop"),
			InstructionSnapshot::Halt => write!(f, "Halt"),
		}
	}
}

impl Display for StateChange {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			StateChange::StackPush {
				index,
				value,
			} => {
				write!(f, "+ stack[{}] = {}", index, value)
			}
			StateChange::StackPop {
				index,
				value,
			} => {
				write!(f, "- stack[{}] = {}", index, value)
			}
			StateChange::PipelinePush {
				index,
				desc,
			} => {
				write!(f, "+ pipeline[{}] = {}", index, desc)
			}
			StateChange::PipelinePop {
				index,
				desc,
			} => {
				write!(f, "- pipeline[{}] = {}", index, desc)
			}
			StateChange::PipelineModify {
				index,
				from,
				to,
			} => {
				write!(f, "~ pipeline[{}] = {} → {}", index, from, to)
			}
			StateChange::VarSet {
				scope_depth,
				name,
				value,
			} => {
				write!(f, "+ scope[{}].{} = {}", scope_depth, name, value)
			}
			StateChange::VarRemove {
				scope_depth,
				name,
				value,
			} => {
				write!(f, "- scope[{}].{} = {}", scope_depth, name, value)
			}
			StateChange::ScopePush {
				depth,
			} => {
				write!(f, "+ scope[{}] (new scope)", depth)
			}
			StateChange::ScopePop {
				depth,
			} => {
				write!(f, "- scope[{}] (scope removed)", depth)
			}
			StateChange::CallPush {
				frame,
			} => {
				write!(f, "+ call[func={}] return=0x{:02X}", frame.function_index, frame.return_address)
			}
			StateChange::CallPop {
				frame,
			} => {
				write!(f, "- call[func={}] return=0x{:02X}", frame.function_index, frame.return_address)
			}
		}
	}
}

impl Display for OperandSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			OperandSnapshot::Scalar(v) => write!(f, "Scalar({:?})", v),
			OperandSnapshot::ExprRef(index) => write!(f, "ExprRef({})", index),
			OperandSnapshot::ColRef(name) => write!(f, "ColRef(\"{}\")", name),
			OperandSnapshot::ColList(cols) => write!(f, "ColList({:?})", cols),
			OperandSnapshot::Frame(frame) => write!(f, "{}", frame),
			OperandSnapshot::FunctionRef(index) => write!(f, "FunctionRef({})", index),
			OperandSnapshot::PipelineRef {
				id,
			} => write!(f, "PipelineRef({})", id),
			OperandSnapshot::SortSpecRef(index) => write!(f, "SortSpecRef({})", index),
			OperandSnapshot::ExtSpecRef(index) => write!(f, "ExtSpecRef({})", index),
			OperandSnapshot::Record(rec) => write!(f, "{}", rec),
		}
	}
}

impl Display for FrameSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Frame({} cols × {} rows)", self.columns.len(), self.row_count)
	}
}

impl Display for RecordSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Record {{ ")?;
		for (i, (name, value)) in self.fields.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{}: {:?}", name, value)?;
		}
		write!(f, " }}")
	}
}

impl Display for CallFrameSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"CallFrame {{ func={}, return=0x{:02X}, op_base={}, pipe_base={}, scope_depth={} }}",
			self.function_index,
			self.return_address,
			self.operand_base,
			self.pipeline_base,
			self.scope_depth
		)
	}
}

/// Format a complete trace (multiple entries).
pub fn format_trace(entries: &[TraceEntry]) -> String {
	let mut output = String::new();
	for entry in entries {
		writeln!(&mut output, "{}", entry).unwrap();
	}
	output
}
