// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
use reifydb_codec::key::encoded::EncodedKey;
#[cfg(test)]
use reifydb_core::interface::store::{EntryKind, classify_key};

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_classify_key_unknown() {
		// An unrecognised prefix must fall back to the catch-all Multi table, never to a typed kind.
		let key = EncodedKey::new(vec![0u8; 10]);
		assert!(matches!(classify_key(&key), EntryKind::Multi));
	}
}
