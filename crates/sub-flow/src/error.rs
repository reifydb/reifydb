// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::flow::{
	extern_abi_tag_mismatch, extern_create_failed, extern_library_load_failed, extern_library_not_loaded,
	extern_operator_not_found, extern_symbol_not_found,
};
use reifydb_value::error::{Diagnostic, IntoDiagnostic, ValueError};

#[derive(Debug, thiserror::Error)]
pub enum SubFlowError {
	#[error("extern operator ABI tag mismatch: plugin {plugin:#06x}, host {host:#06x}")]
	AbiTagMismatch {
		plugin: u32,
		host: u32,
	},

	#[error("extern operator library not loaded: {path}")]
	LibraryNotLoaded {
		path: String,
	},

	#[error("extern operator library failed to load: {path}: {cause}")]
	LibraryLoadFailed {
		path: String,
		cause: String,
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

impl IntoDiagnostic for SubFlowError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			SubFlowError::AbiTagMismatch {
				plugin,
				host,
			} => extern_abi_tag_mismatch(plugin, host),
			SubFlowError::LibraryNotLoaded {
				path,
			} => extern_library_not_loaded(&path),
			SubFlowError::LibraryLoadFailed {
				path,
				cause,
			} => extern_library_load_failed(&path, cause),
			SubFlowError::SymbolNotFound {
				symbol,
				cause,
			} => extern_symbol_not_found(symbol, cause),
			SubFlowError::OperatorNotFound {
				operator,
			} => extern_operator_not_found(&operator),
			SubFlowError::CreateFailed {
				cause,
			} => extern_create_failed(cause),
		}
	}
}

impl From<SubFlowError> for ValueError {
	fn from(err: SubFlowError) -> Self {
		ValueError(Box::new(err.into_diagnostic()))
	}
}
