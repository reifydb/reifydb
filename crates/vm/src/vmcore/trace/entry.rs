// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Trace entry types for VM execution tracing.

use std::fmt;

use reifydb_type::Value;

/// A single trace entry capturing VM state after executing an instruction.
#[derive(Debug, Clone)]
pub struct TraceEntry {
	/// Sequential step number (0-indexed).
	pub step: usize,

	/// Instruction pointer before this step executed.
	pub ip_before: usize,

	/// Instruction pointer after this step executed.
	pub ip_after: usize,

	/// Raw bytecode bytes for this instruction.
	pub bytecode: Vec<u8>,

	/// Human-readable decoded instruction.
	pub instruction: InstructionSnapshot,

	/// Changes from the previous state (delta).
	pub changes: Vec<StateChange>,

	/// Full VM state after execution.
	pub state: StateSnapshot,

	/// Result of the dispatch.
	pub result: DispatchResultSnapshot,
}

/// Snapshot of a decoded instruction for display.
#[derive(Debug, Clone)]
pub enum InstructionSnapshot {
	PushConst {
		index: u16,
		value: Value,
	},
	PushExpr {
		index: u16,
	},
	PushColRef {
		name: String,
	},
	PushColList {
		columns: Vec<String>,
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
		name: String,
	},
	StoreVar {
		name: String,
	},
	StorePipeline {
		name: String,
	},
	LoadPipeline {
		name: String,
	},
	UpdateVar {
		name: String,
	},
	Source {
		index: u16,
		name: String,
	},
	Inline,
	Apply {
		operator: OperatorSnapshot,
	},
	Collect,
	PopPipeline,
	Merge,
	DupPipeline,
	Jump {
		offset: i16,
		target: usize,
	},
	JumpIf {
		offset: i16,
		target: usize,
	},
	JumpIfNot {
		offset: i16,
		target: usize,
	},
	Call {
		func_index: u16,
	},
	Return,
	CallBuiltin {
		builtin_id: u16,
		arg_count: u8,
	},
	EnterScope,
	ExitScope,
	FrameLen,
	FrameRow,
	GetField {
		name: String,
	},
	IntAdd,
	IntLt,
	IntEq,
	PrintOut,
	Nop,
	Halt,
}

/// Operator kind for Apply instruction.
#[derive(Debug, Clone, Copy)]
pub enum OperatorSnapshot {
	Filter,
	Select,
	Extend,
	Take,
	Sort,
}

impl fmt::Display for OperatorSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			OperatorSnapshot::Filter => write!(f, "Filter"),
			OperatorSnapshot::Select => write!(f, "Select"),
			OperatorSnapshot::Extend => write!(f, "Extend"),
			OperatorSnapshot::Take => write!(f, "Take"),
			OperatorSnapshot::Sort => write!(f, "Sort"),
		}
	}
}

/// State change (delta) from previous step.
#[derive(Debug, Clone)]
pub enum StateChange {
	/// Value pushed onto operand stack.
	StackPush {
		index: usize,
		value: OperandSnapshot,
	},

	/// Value popped from operand stack.
	StackPop {
		index: usize,
		value: OperandSnapshot,
	},

	/// Pipeline pushed onto pipeline stack.
	PipelinePush {
		index: usize,
		desc: String,
	},

	/// Pipeline popped from pipeline stack.
	PipelinePop {
		index: usize,
		desc: String,
	},

	/// Pipeline modified in-place (e.g., filter applied).
	PipelineModify {
		index: usize,
		from: String,
		to: String,
	},

	/// Variable set in scope.
	VarSet {
		scope_depth: usize,
		name: String,
		value: OperandSnapshot,
	},

	/// Variable removed from scope.
	VarRemove {
		scope_depth: usize,
		name: String,
		value: OperandSnapshot,
	},

	/// New scope pushed.
	ScopePush {
		depth: usize,
	},

	/// Scope popped.
	ScopePop {
		depth: usize,
	},

	/// Call frame pushed.
	CallPush {
		frame: CallFrameSnapshot,
	},

	/// Call frame popped.
	CallPop {
		frame: CallFrameSnapshot,
	},
}

/// Snapshot of an operand value (cloneable representation).
#[derive(Debug, Clone)]
pub enum OperandSnapshot {
	/// Scalar value.
	Scalar(Value),

	/// Expression reference.
	ExprRef(u16),

	/// Column reference by name.
	ColRef(String),

	/// List of column names.
	ColList(Vec<String>),

	/// Materialized frame with metadata.
	Frame(FrameSnapshot),

	/// Function reference.
	FunctionRef(u16),

	/// Pipeline reference.
	PipelineRef {
		id: u64,
	},

	/// Sort specification reference.
	SortSpecRef(u16),

	/// Extension specification reference.
	ExtSpecRef(u16),

	/// Record (single row).
	Record(RecordSnapshot),
}

/// Snapshot of a materialized frame (columns).
#[derive(Debug, Clone)]
pub struct FrameSnapshot {
	/// Number of rows.
	pub row_count: usize,

	/// Column metadata.
	pub columns: Vec<ColumnSnapshot>,

	/// All row data.
	pub rows: Vec<Vec<Value>>,
}

/// Snapshot of a column's metadata.
#[derive(Debug, Clone)]
pub struct ColumnSnapshot {
	/// Column name.
	pub name: String,

	/// Column data type as string.
	pub data_type: String,
}

/// Snapshot of a record (single row with named fields).
#[derive(Debug, Clone)]
pub struct RecordSnapshot {
	/// Field name -> value pairs.
	pub fields: Vec<(String, Value)>,
}

/// Complete snapshot of VM state at a point in time.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
	/// Instruction pointer.
	pub ip: usize,

	/// Operand stack contents.
	pub operand_stack: Vec<OperandSnapshot>,

	/// Pipeline stack descriptions.
	pub pipeline_stack: Vec<String>,

	/// Variable scopes.
	pub scopes: Vec<ScopeSnapshot>,

	/// Call stack frames.
	pub call_stack: Vec<CallFrameSnapshot>,
}

impl StateSnapshot {
	/// Create an empty initial state snapshot.
	pub fn empty() -> Self {
		Self {
			ip: 0,
			operand_stack: Vec::new(),
			pipeline_stack: Vec::new(),
			scopes: vec![ScopeSnapshot {
				depth: 0,
				variables: Vec::new(),
			}],
			call_stack: Vec::new(),
		}
	}
}

/// Snapshot of a single scope level.
#[derive(Debug, Clone)]
pub struct ScopeSnapshot {
	/// Depth level (0 = global).
	pub depth: usize,

	/// Variables in this scope.
	pub variables: Vec<(String, OperandSnapshot)>,
}

/// Snapshot of a call frame.
#[derive(Debug, Clone)]
pub struct CallFrameSnapshot {
	/// Function index being executed.
	pub function_index: u16,

	/// Return address.
	pub return_address: usize,

	/// Operand stack base.
	pub operand_base: usize,

	/// Pipeline stack base.
	pub pipeline_base: usize,

	/// Scope depth at call time.
	pub scope_depth: usize,
}

/// Result of dispatching an instruction.
#[derive(Debug, Clone, Copy)]
pub enum DispatchResultSnapshot {
	/// Continue to next instruction.
	Continue,

	/// Halt execution.
	Halt,

	/// Yield a pipeline result.
	Yield,
}

impl fmt::Display for DispatchResultSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			DispatchResultSnapshot::Continue => write!(f, "Continue"),
			DispatchResultSnapshot::Halt => write!(f, "Halt"),
			DispatchResultSnapshot::Yield => write!(f, "Yield"),
		}
	}
}
