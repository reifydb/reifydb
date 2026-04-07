// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::fingerprint::{RequestFingerprint, StatementFingerprint};
use reifydb_runtime::hash::xxh3_128;

/// Compute a request fingerprint from individual statement fingerprints.
///
/// The request fingerprint combines all statement fingerprints in order,
/// so the same set of statements always produces the same request fingerprint.
pub fn fingerprint_request(statements: &[StatementFingerprint]) -> RequestFingerprint {
	let mut buf = Vec::with_capacity(statements.len() * 16);
	for fp in statements {
		buf.extend_from_slice(&StatementFingerprint::to_le_bytes(fp));
	}
	RequestFingerprint(xxh3_128(&buf))
}
