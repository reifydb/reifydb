// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::flow::{
	flow_guest_key_too_wide, flow_state_decode_failed, flow_state_encode_failed,
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
