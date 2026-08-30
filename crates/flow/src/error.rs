// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::flow::{
	flow_extern_unsupported_on_wasm, flow_guest_key_too_wide, flow_missing_input_edge, flow_operator_input_arity,
	flow_parent_operator_not_found, flow_sink_dictionary_not_found, flow_sink_missing_series_key,
	flow_sink_missing_system_column, flow_sink_not_a_source_family, flow_span_on_unageable_node,
	flow_state_decode_failed, flow_state_encode_failed, flow_unknown_diff_origin, flow_unknown_operator,
	flow_unsupported_operator,
};
use reifydb_value::error::{Diagnostic, Error, IntoDiagnostic};

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

	#[error("a guest row mapping key is {len} bytes, the key holds at most 16")]
	GuestKeyTooWide {
		len: usize,
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
			FlowStateError::GuestKeyTooWide {
				len,
			} => flow_guest_key_too_wide(len),
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
	ExternUnsupportedOnWasm,

	#[error("flow operator is missing a required input edge")]
	MissingInputEdge,

	#[error("{operator} operator received a diff from an unknown operator")]
	UnknownDiffOrigin {
		operator: &'static str,
		origin: Option<String>,
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
			FlowGraphError::ExternUnsupportedOnWasm => flow_extern_unsupported_on_wasm(),
			FlowGraphError::MissingInputEdge => flow_missing_input_edge(),
			FlowGraphError::UnknownDiffOrigin {
				operator,
				origin,
			} => flow_unknown_diff_origin(operator, origin),
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

	#[error("a view sink cannot encode a row of the {family} family")]
	NotASourceFamily {
		family: String,
	},

	#[error("row at index {row_idx} of view '{view}' has no series key in column '{column}'")]
	MissingSeriesKey {
		view: String,
		column: String,
		row_idx: usize,
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
			FlowSinkError::NotASourceFamily {
				family,
			} => flow_sink_not_a_source_family(&family),
			FlowSinkError::MissingSeriesKey {
				view,
				column,
				row_idx,
			} => flow_sink_missing_series_key(&view, &column, row_idx),
		}
	}
}

impl From<FlowSinkError> for Error {
	fn from(err: FlowSinkError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
