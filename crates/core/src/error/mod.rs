// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Error, IntoDiagnostic, TypeError},
	fragment::Fragment,
	value::{duration::Duration, value_type::ValueType},
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

	#[error("operator setting '{key}' is a count ({count}), but this operator seals by time")]
	OperatorWithCountSpan {
		key: &'static str,
		count: u64,
	},

	#[error("immutable {immutable} must be strictly less than lateness {lateness}")]
	OperatorWithImmutableNotBelowLateness {
		immutable: Duration,
		lateness: Duration,
	},

	#[error("this operator needs 'window' in its with block")]
	OperatorWithWindowMissing,

	#[error("window '{kind}' is not supported by this operator, use '{supported}'")]
	OperatorWithWindowKindUnsupported {
		kind: &'static str,
		supported: &'static str,
	},

	#[error("this operator takes no window, but window '{kind}' was given")]
	OperatorWithWindowNotSupported {
		kind: &'static str,
	},

	#[error("the window size is a count ({count}), but this operator windows by time")]
	OperatorWithWindowSizeCount {
		count: u64,
	},

	#[error("the window size is a duration ({size}), but this operator counts slots")]
	OperatorWithWindowSizeDuration {
		size: Duration,
	},

	#[error("operator setting '{key}' is a duration ({duration}), but this operator seals by count")]
	OperatorWithDurationSpan {
		key: &'static str,
		duration: Duration,
	},

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

	#[error("cannot use column `{column}` in a primary key: a {ty} value cannot be a key")]
	PrimaryKeyDigestColumn {
		fragment: Fragment,
		column: String,
		ty: ValueType,
	},

	#[error("expected {expected}, got {actual}")]
	DigestWriteTypeMismatch {
		fragment: Fragment,
		expected: ValueType,
		actual: ValueType,
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
		value_type: ValueType,
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
		Error(Box::new(err.into_diagnostic()))
	}
}
