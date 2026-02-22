// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error::{Error, IntoDiagnostic, TypeError},
	fragment::Fragment,
	value::r#type::Type,
};

pub mod diagnostic;

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
	#[error(transparent)]
	Type(#[from] TypeError),

	#[error("variable-length types (UTF8, BLOB) are not supported in indexes")]
	IndexVariableLengthNotSupported,

	#[error("mismatch between number of types ({types_len}) and directions ({directions_len})")]
	IndexTypesDirectionsMismatch {
		types_len: usize,
		directions_len: usize,
	},

	#[error("Frame processing error: {message}")]
	FrameError {
		message: String,
	},

	#[error("Flow processing error: {message}")]
	FlowError {
		message: String,
	},

	#[error("FlowTransaction keyspace overlap: key {key} was already written")]
	FlowTransactionKeyspaceOverlap {
		key: String,
	},

	#[error("Flow {flow_id} is already registered")]
	FlowAlreadyRegistered {
		flow_id: u64,
	},

	#[error("Flow {flow_id} version data is corrupted")]
	FlowVersionCorrupted {
		flow_id: u64,
		byte_count: usize,
	},

	#[error("Timeout waiting for flow {flow_id} backfill")]
	FlowBackfillTimeout {
		flow_id: u64,
		timeout_secs: u64,
	},

	#[error("Flow dispatcher is unavailable")]
	FlowDispatcherUnavailable,

	#[error("Primary key violation in table '{table_name}'")]
	PrimaryKeyViolation {
		fragment: Fragment,
		table_name: String,
		key_columns: Vec<String>,
	},

	#[error("Unique index violation in index '{index_name}' on table '{table_name}'")]
	UniqueIndexViolation {
		fragment: Fragment,
		table_name: String,
		index_name: String,
		key_columns: Vec<String>,
	},

	#[error("Internal error: {message}")]
	Internal {
		message: String,
		file: String,
		line: u32,
		column: u32,
		function: String,
		module_path: String,
	},

	#[error("{component} is shutting down")]
	Shutdown {
		component: String,
	},

	#[error("sequence generator of type `{value_type}` is exhausted")]
	SequenceExhausted {
		value_type: Type,
	},

	#[error("cannot alter sequence for non-AUTO INCREMENT column")]
	CanNotAlterNotAutoIncrement {
		fragment: Fragment,
	},

	#[error("{subsystem} subsystem initialization failed: {reason}")]
	SubsystemInitFailed {
		subsystem: String,
		reason: String,
	},

	#[error("Required feature '{feature}' is not enabled")]
	SubsystemFeatureDisabled {
		feature: String,
	},

	#[error("Failed to bind to {addr}: {reason}")]
	SubsystemBindFailed {
		addr: String,
		reason: String,
	},

	#[error("{subsystem} subsystem shutdown failed: {reason}")]
	SubsystemShutdownFailed {
		subsystem: String,
		reason: String,
	},

	#[error("Failed to get local address: {reason}")]
	SubsystemAddressUnavailable {
		reason: String,
	},

	#[error("Socket configuration failed: {reason}")]
	SubsystemSocketConfigFailed {
		reason: String,
	},
}

impl From<CoreError> for Error {
	fn from(err: CoreError) -> Self {
		Error(err.into_diagnostic())
	}
}
