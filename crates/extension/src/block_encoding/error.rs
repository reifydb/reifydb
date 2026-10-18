// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::ColumnBlockId;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum BlockEncodingError {
	#[error("block encoding '{name}' is not registered")]
	UnknownName {
		name: String,
		fragment: Fragment,
	},

	#[error("block written by '{name}' v{stored}, this build supports up to v{supported}")]
	NewerVersion {
		name: String,
		stored: u32,
		supported: u32,
	},

	#[error("'{name}' changed the row count from {before} to {after}")]
	RowCountChanged {
		name: String,
		before: usize,
		after: usize,
	},

	#[error("'{name}' does not keep column '{column}' with its name and type")]
	PassThroughFieldChanged {
		name: String,
		column: String,
	},

	#[error("'{name}' changed the values of column '{column}'")]
	PassThroughValuesChanged {
		name: String,
		column: String,
	},

	#[error("'{name}' produced fields that differ from its stored_fields()")]
	StoredFieldsDiffer {
		name: String,
	},

	#[error("'{name}' decoded fields that differ from the user columns of the block")]
	DecodedFieldsDiffer {
		name: String,
	},

	#[error("'{name}' stored field '{field}' has no value type")]
	UnsupportedStoredType {
		name: String,
		field: String,
	},

	#[error("invalid config for '{name}': {reason}")]
	InvalidConfig {
		name: String,
		reason: String,
	},

	#[error("encoded block '{name}' in block {block} reached a scan without a decoder")]
	NoDecoder {
		name: String,
		block: ColumnBlockId,
	},

	#[error("series '{series}' user fields at seal differ from its catalog definition for encoding '{name}'")]
	UserFieldsDiffer {
		name: String,
		series: String,
	},
}

impl From<BlockEncodingError> for Error {
	fn from(err: BlockEncodingError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for BlockEncodingError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			BlockEncodingError::UnknownName {
				name,
				fragment,
			} => Diagnostic {
				code: "BENC_001".to_string(),
				rql: None,
				message: format!("block encoding '{name}' is not registered"),
				column: None,
				fragment,
				label: Some("block encoding not registered".to_string()),
				help: Some("register the encoding on the database builder with with_block_encodings before using it".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::NewerVersion {
				name,
				stored,
				supported,
			} => Diagnostic {
				code: "BENC_002".to_string(),
				rql: None,
				message: format!("block written by '{name}' v{stored}, this build supports up to v{supported}"),
				column: None,
				fragment: Fragment::None,
				label: Some("block written by a newer encoding version".to_string()),
				help: Some("read this block with a build whose encoding supports the stored version".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::RowCountChanged {
				name,
				before,
				after,
			} => Diagnostic {
				code: "BENC_003".to_string(),
				rql: None,
				message: format!("'{name}' changed the row count from {before} to {after}"),
				column: None,
				fragment: Fragment::None,
				label: Some("encoding changed the row count".to_string()),
				help: Some("encode and decode must keep every row, in order".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::PassThroughFieldChanged {
				name,
				column,
			} => Diagnostic {
				code: "BENC_004".to_string(),
				rql: None,
				message: format!("'{name}' does not keep column '{column}' with its name and type"),
				column: None,
				fragment: Fragment::None,
				label: Some("encoding does not keep a pass-through column".to_string()),
				help: Some("the key and tag columns must pass through the encoding with the same name and type".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::PassThroughValuesChanged {
				name,
				column,
			} => Diagnostic {
				code: "BENC_005".to_string(),
				rql: None,
				message: format!("'{name}' changed the values of column '{column}'"),
				column: None,
				fragment: Fragment::None,
				label: Some("encoding changed pass-through values".to_string()),
				help: Some("the key and tag columns must pass through the encoding with the same values".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::StoredFieldsDiffer {
				name,
			} => Diagnostic {
				code: "BENC_006".to_string(),
				rql: None,
				message: format!("'{name}' produced fields that differ from its stored_fields()"),
				column: None,
				fragment: Fragment::None,
				label: Some("encoded fields differ from stored_fields()".to_string()),
				help: Some("encode must return exactly the fields that stored_fields() declares".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::UnsupportedStoredType {
				name,
				field,
			} => Diagnostic {
				code: "BENC_007".to_string(),
				rql: None,
				message: format!("'{name}' stored field '{field}' has no value type"),
				column: None,
				fragment: Fragment::None,
				label: Some("stored field has no value type".to_string()),
				help: Some("stored fields must use arrow types that map to a value type".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::InvalidConfig {
				name,
				reason,
			} => Diagnostic {
				code: "BENC_008".to_string(),
				rql: None,
				message: format!("invalid config for '{name}': {reason}"),
				column: None,
				fragment: Fragment::None,
				label: Some("invalid encoding config".to_string()),
				help: Some("check the keys and values given to the encoding in the series WITH clause".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::NoDecoder {
				name,
				block,
			} => Diagnostic {
				code: "BENC_009".to_string(),
				rql: None,
				message: format!("encoded block '{name}' in block {block} reached a scan without a decoder"),
				column: None,
				fragment: Fragment::None,
				label: Some("encoded block without a decoder".to_string()),
				help: Some("this scan path cannot read encoded blocks".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::DecodedFieldsDiffer {
				name,
			} => Diagnostic {
				code: "BENC_011".to_string(),
				rql: None,
				message: format!("'{name}' decoded fields that differ from the user columns of the block"),
				column: None,
				fragment: Fragment::None,
				label: Some("decoded fields differ from the block header".to_string()),
				help: Some("decode must return exactly the user columns recorded in the block header, in order".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			BlockEncodingError::UserFieldsDiffer {
				name,
				series,
			} => Diagnostic {
				code: "BENC_010".to_string(),
				rql: None,
				message: format!("series '{series}' user fields at seal differ from its catalog definition for encoding '{name}'"),
				column: None,
				fragment: Fragment::None,
				label: Some("user fields differ from the catalog".to_string()),
				help: Some("the sealed block must have exactly the user fields the catalog defines for the series".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
