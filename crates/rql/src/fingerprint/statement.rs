// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::fingerprint::StatementFingerprint;
use reifydb_runtime::hash::xxh3_128;

use super::walk::{FingerprintBuffer, fingerprint_ast_slice};
use crate::ast::ast::AstStatement;

/// Compute a fingerprint for a single parsed statement.
///
/// The fingerprint captures the structural shape of the query (node types,
/// identifiers, operators) while normalizing away literal values. Two queries
/// that differ only in constants produce the same fingerprint.
pub fn fingerprint_statement(statement: &AstStatement<'_>) -> StatementFingerprint {
	let mut buf = FingerprintBuffer::new();
	buf.write_u8(statement.has_pipes as u8);
	buf.write_u8(statement.is_output as u8);
	fingerprint_ast_slice(&mut buf, &statement.nodes);
	StatementFingerprint(xxh3_128(buf.as_bytes()))
}
