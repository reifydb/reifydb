// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod create;
pub mod find;
pub mod get;
pub(crate) mod layout;
pub mod list;

use reifydb_core::{
	encoded::encoded::EncodedValues,
	retention::{CleanupMode, RetentionPolicy},
};

use self::layout::retention_policy;

/// Encode a RetentionPolicy into EncodedValues
pub(crate) fn encode_retention_policy(policy: &RetentionPolicy) -> EncodedValues {
	let mut row = retention_policy::LAYOUT.allocate();

	match policy {
		RetentionPolicy::KeepForever => {
			retention_policy::LAYOUT.set_u8(
				&mut row,
				retention_policy::POLICY_TYPE,
				retention_policy::POLICY_KEEP_FOREVER,
			);
			// Other fields remain 0/default
		}
		RetentionPolicy::KeepVersions {
			count,
			cleanup_mode,
		} => {
			retention_policy::LAYOUT.set_u8(
				&mut row,
				retention_policy::POLICY_TYPE,
				retention_policy::POLICY_KEEP_VERSIONS,
			);
			retention_policy::LAYOUT.set_u8(
				&mut row,
				retention_policy::CLEANUP_MODE,
				encode_cleanup_mode(cleanup_mode),
			);
			retention_policy::LAYOUT.set_u64(&mut row, retention_policy::VALUE, *count);
		}
	}

	row
}

/// Decode a RetentionPolicy from EncodedValues
pub(crate) fn decode_retention_policy(row: &EncodedValues) -> Option<RetentionPolicy> {
	let policy_type = retention_policy::LAYOUT.get_u8(row, retention_policy::POLICY_TYPE);

	match policy_type {
		retention_policy::POLICY_KEEP_FOREVER => Some(RetentionPolicy::KeepForever),

		retention_policy::POLICY_KEEP_VERSIONS => {
			let cleanup_mode = decode_cleanup_mode(
				retention_policy::LAYOUT.get_u8(row, retention_policy::CLEANUP_MODE),
			)?;
			let count = retention_policy::LAYOUT.get_u64(row, retention_policy::VALUE);
			Some(RetentionPolicy::KeepVersions {
				count,
				cleanup_mode,
			})
		}

		_ => None,
	}
}

fn encode_cleanup_mode(mode: &CleanupMode) -> u8 {
	match mode {
		CleanupMode::Delete => retention_policy::CLEANUP_MODE_DELETE,
		CleanupMode::Drop => retention_policy::CLEANUP_MODE_DROP,
	}
}

fn decode_cleanup_mode(mode: u8) -> Option<CleanupMode> {
	match mode {
		retention_policy::CLEANUP_MODE_DELETE => Some(CleanupMode::Delete),
		retention_policy::CLEANUP_MODE_DROP => Some(CleanupMode::Drop),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_keep_forever() {
		let policy = RetentionPolicy::KeepForever;
		let encoded = encode_retention_policy(&policy);
		let decoded = decode_retention_policy(&encoded).unwrap();
		assert_eq!(policy, decoded);
	}

	#[test]
	fn test_encode_decode_keep_versions() {
		let policy = RetentionPolicy::KeepVersions {
			count: 100,
			cleanup_mode: CleanupMode::Delete,
		};
		let encoded = encode_retention_policy(&policy);
		let decoded = decode_retention_policy(&encoded).unwrap();
		assert_eq!(policy, decoded);
	}
}
