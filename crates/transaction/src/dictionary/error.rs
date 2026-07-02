// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, IntoDiagnostic},
	fragment::Fragment,
	value::dictionary::DictionaryId,
};

#[derive(Debug, thiserror::Error)]
pub enum DictionaryError {
	#[error("dictionary {dictionary} entry record for hash {hash:02x?} is truncated ({len} bytes)")]
	TruncatedEntry {
		dictionary: DictionaryId,
		hash: [u8; 16],
		len: usize,
	},

	#[error("dictionary {dictionary} hash collision on {hash:02x?}: two distinct values share one 128-bit hash")]
	HashCollision {
		dictionary: DictionaryId,
		hash: [u8; 16],
	},

	#[error("dictionary {dictionary} is being dropped; interning is not allowed")]
	Dropped {
		dictionary: DictionaryId,
	},

	#[error("dictionary {dictionary} id space exhausted")]
	Exhausted {
		dictionary: DictionaryId,
	},
}

impl IntoDiagnostic for DictionaryError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			DictionaryError::TruncatedEntry {
				dictionary,
				hash,
				len,
			} => Diagnostic {
				code: "TXN_015".to_string(),
				rql: None,
				message: format!(
					"dictionary {} entry record for hash {:02x?} is truncated ({} bytes)",
					dictionary, hash, len
				),
				column: None,
				fragment: Fragment::None,
				label: Some("dictionary entry record is truncated".to_string()),
				help: Some(
					"The stored entry is shorter than its 16-byte id prefix, so its id cannot be decoded. This indicates dictionary storage corruption; report it as a bug."
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			DictionaryError::HashCollision {
				dictionary,
				hash,
			} => Diagnostic {
				code: "TXN_016".to_string(),
				rql: None,
				message: format!(
					"dictionary {} hash collision on {:02x?}: two distinct values share one 128-bit hash",
					dictionary, hash
				),
				column: None,
				fragment: Fragment::None,
				label: Some("128-bit hash collision".to_string()),
				help: Some(
					"Two distinct values produced the same 128-bit hash. This is astronomically unlikely and points to a hashing bug or data corruption; report it as a bug."
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			DictionaryError::Dropped {
				dictionary,
			} => Diagnostic {
				code: "TXN_017".to_string(),
				rql: None,
				message: format!("dictionary {} is being dropped; interning is not allowed", dictionary),
				column: None,
				fragment: Fragment::None,
				label: Some("dictionary is being dropped".to_string()),
				help: Some("The dictionary is being dropped and no longer accepts new entries.".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			DictionaryError::Exhausted {
				dictionary,
			} => Diagnostic {
				code: "TXN_018".to_string(),
				rql: None,
				message: format!("dictionary {} id space exhausted", dictionary),
				column: None,
				fragment: Fragment::None,
				label: Some("dictionary id space exhausted".to_string()),
				help: Some(
					"The dictionary's id counter reached its maximum; no further entries can be allocated."
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
