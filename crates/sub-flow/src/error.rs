// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::error::CdcError;
use reifydb_core::error::diagnostic::flow::{
	flow_catch_up_read_failed, native_abi_tag_mismatch, native_create_failed, native_library_not_loaded,
	native_operator_not_found, native_symbol_not_found,
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
pub enum ExternOperatorError {
	#[error("extern operator ABI tag mismatch: plugin {plugin:#06x}, host {host:#06x}")]
	AbiTagMismatch {
		plugin: u32,
		host: u32,
	},

	#[error("extern operator library not loaded: {path}")]
	LibraryNotLoaded {
		path: String,
	},

	#[error("extern operator symbol '{symbol}' not found: {cause}")]
	SymbolNotFound {
		symbol: &'static str,
		cause: String,
	},

	#[error("extern operator '{operator}' not found")]
	OperatorNotFound {
		operator: String,
	},

	#[error("failed to create extern operator: {cause}")]
	CreateFailed {
		cause: String,
	},
}

impl IntoDiagnostic for ExternOperatorError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			ExternOperatorError::AbiTagMismatch {
				plugin,
				host,
			} => native_abi_tag_mismatch(plugin, host),
			ExternOperatorError::LibraryNotLoaded {
				path,
			} => native_library_not_loaded(&path),
			ExternOperatorError::SymbolNotFound {
				symbol,
				cause,
			} => native_symbol_not_found(symbol, cause),
			ExternOperatorError::OperatorNotFound {
				operator,
			} => native_operator_not_found(&operator),
			ExternOperatorError::CreateFailed {
				cause,
			} => native_create_failed(cause),
		}
	}
}

impl From<ExternOperatorError> for Error {
	fn from(err: ExternOperatorError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
