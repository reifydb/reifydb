// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::error::CdcError;
use reifydb_core::error::diagnostic::flow::{
	flow_catch_up_read_failed, flow_ffi_unsupported_on_wasm, flow_missing_input_edge, flow_operator_input_arity,
	flow_parent_operator_not_found, flow_sink_dictionary_not_found, flow_sink_missing_system_column,
	flow_span_on_unageable_node, flow_span_without_reclaim, flow_state_decode_failed, flow_state_encode_failed,
	flow_unknown_diff_origin, flow_unknown_operator, flow_unsupported_operator, native_abi_tag_mismatch,
	native_create_failed, native_library_not_loaded, native_operator_not_found, native_symbol_not_found,
};
use reifydb_value::error::{Diagnostic, Error, IntoDiagnostic};

#[derive(Debug, thiserror::Error)]
pub enum FlowLoadError {
	#[error("cdc catch-up read for versions ({from}, {up_to}] failed: {cause}")]
	Read {
		from: u64,
		up_to: u64,
		cause: CdcError,
	},
}

impl IntoDiagnostic for FlowLoadError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			FlowLoadError::Read {
				from,
				up_to,
				cause,
			} => flow_catch_up_read_failed(from, up_to, &cause.to_string()),
		}
	}
}

impl From<FlowLoadError> for Error {
	fn from(err: FlowLoadError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[derive(Debug, thiserror::Error)]
pub enum FlowStateError {
	#[error("failed to serialize flow operator state '{state}': {cause}")]
	Encode {
		state: &'static str,
		cause: String,
	},

	#[error("failed to deserialize flow operator state '{state}': {cause}")]
	Decode {
		state: &'static str,
		cause: String,
	},
}

impl IntoDiagnostic for FlowStateError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			FlowStateError::Encode {
				state,
				cause,
			} => flow_state_encode_failed(state, cause),
			FlowStateError::Decode {
				state,
				cause,
			} => flow_state_decode_failed(state, cause),
		}
	}
}

impl From<FlowStateError> for Error {
	fn from(err: FlowStateError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[derive(Debug, thiserror::Error)]
pub enum FlowGraphError {
	#[error("flow operator kind '{kind}' is not supported in persistent flows")]
	UnsupportedNode {
		kind: &'static str,
	},

	#[error("flow operator '{operator}' requires {expected} inputs, but the DAG provided {found}")]
	NodeInputArity {
		operator: &'static str,
		expected: &'static str,
		found: usize,
	},

	#[error("parent operator not found while wiring flow operator input: {input}")]
	ParentOperatorNotFound {
		input: String,
	},

	#[error("unknown flow operator '{operator}'")]
	UnknownOperator {
		operator: String,
	},

	#[error("FFI operators are not supported on the wasm target")]
	FfiUnsupportedOnWasm,

	#[error("flow operator is missing a required input edge")]
	MissingInputEdge,

	#[error("{operator} operator received a diff from an unknown operator")]
	UnknownDiffOrigin {
		operator: &'static str,
		origin: Option<String>,
	},

	#[error("{operator} in flow {flow_id} declares a retention span but cannot reclaim")]
	SpanWithoutReclaim {
		flow_id: u64,
		operator: String,
	},

	#[error("{operator} in flow {flow_id} declares a retention span but holds no state to age")]
	SpanOnUnageableNode {
		flow_id: u64,
		operator: String,
	},
}

impl IntoDiagnostic for FlowGraphError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			FlowGraphError::UnsupportedNode {
				kind,
			} => flow_unsupported_operator(kind),
			FlowGraphError::NodeInputArity {
				operator,
				expected,
				found,
			} => flow_operator_input_arity(operator, expected, found),
			FlowGraphError::ParentOperatorNotFound {
				input,
			} => flow_parent_operator_not_found(input),
			FlowGraphError::UnknownOperator {
				operator,
			} => flow_unknown_operator(&operator),
			FlowGraphError::FfiUnsupportedOnWasm => flow_ffi_unsupported_on_wasm(),
			FlowGraphError::MissingInputEdge => flow_missing_input_edge(),
			FlowGraphError::UnknownDiffOrigin {
				operator,
				origin,
			} => flow_unknown_diff_origin(operator, origin),
			FlowGraphError::SpanWithoutReclaim {
				flow_id,
				operator,
			} => flow_span_without_reclaim(&format!("flow {flow_id}"), operator.as_str()),
			FlowGraphError::SpanOnUnageableNode {
				flow_id,
				operator,
			} => flow_span_on_unageable_node(&format!("flow {flow_id}"), operator.as_str()),
		}
	}
}

impl From<FlowGraphError> for Error {
	fn from(err: FlowGraphError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[derive(Debug, thiserror::Error)]
pub enum NativeOperatorError {
	#[error("native operator ABI tag mismatch: plugin {plugin:#06x}, host {host:#06x}")]
	AbiTagMismatch {
		plugin: u32,
		host: u32,
	},

	#[error("native operator library not loaded: {path}")]
	LibraryNotLoaded {
		path: String,
	},

	#[error("native operator symbol '{symbol}' not found: {cause}")]
	SymbolNotFound {
		symbol: &'static str,
		cause: String,
	},

	#[error("native operator '{operator}' not found")]
	OperatorNotFound {
		operator: String,
	},

	#[error("failed to create native/FFI operator: {cause}")]
	CreateFailed {
		cause: String,
	},
}

impl IntoDiagnostic for NativeOperatorError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			NativeOperatorError::AbiTagMismatch {
				plugin,
				host,
			} => native_abi_tag_mismatch(plugin, host),
			NativeOperatorError::LibraryNotLoaded {
				path,
			} => native_library_not_loaded(&path),
			NativeOperatorError::SymbolNotFound {
				symbol,
				cause,
			} => native_symbol_not_found(symbol, cause),
			NativeOperatorError::OperatorNotFound {
				operator,
			} => native_operator_not_found(&operator),
			NativeOperatorError::CreateFailed {
				cause,
			} => native_create_failed(cause),
		}
	}
}

impl From<NativeOperatorError> for Error {
	fn from(err: NativeOperatorError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[derive(Debug, thiserror::Error)]
pub enum FlowSinkError {
	#[error("row at index {row_idx} is missing the '{column}' system column")]
	MissingSystemColumn {
		column: &'static str,
		row_idx: usize,
	},

	#[error("dictionary {dictionary_id} not found for view column '{column}'")]
	DictionaryNotFound {
		dictionary_id: String,
		column: String,
	},
}

impl IntoDiagnostic for FlowSinkError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			FlowSinkError::MissingSystemColumn {
				column,
				row_idx,
			} => flow_sink_missing_system_column(column, row_idx),
			FlowSinkError::DictionaryNotFound {
				dictionary_id,
				column,
			} => flow_sink_dictionary_not_found(dictionary_id, &column),
		}
	}
}

impl From<FlowSinkError> for Error {
	fn from(err: FlowSinkError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
