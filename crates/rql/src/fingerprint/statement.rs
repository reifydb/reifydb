// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::fingerprint::StatementFingerprint;
use reifydb_runtime::hash::xxh3_128;

use super::walk::{FingerprintBuffer, fingerprint_ast_slice};
use crate::{
	ast::ast::AstStatement,
	bump::{Bump, BumpFragment},
	token::{token::TokenKind, tokenize},
};

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

/// Normalizes a statement string by replacing literal values with `?`.
pub fn normalize_statement(statement: &AstStatement<'_>) -> String {
	let bump = Bump::new();
	let tokens = match tokenize(&bump, statement.rql) {
		Ok(t) => t,
		Err(_) => return statement.rql.to_string(),
	};

	let mut normalized = String::with_capacity(statement.rql.len());
	let mut last_end = 0;

	for token in tokens.iter() {
		if let BumpFragment::Statement {
			offset,
			source_end,
			..
		} = token.fragment
		{
			// Add any whitespace before this token
			if offset > last_end {
				normalized.push_str(&statement.rql[last_end..offset]);
			}
			if matches!(token.kind, TokenKind::Literal(_)) {
				normalized.push('?');
			} else {
				normalized.push_str(&statement.rql[offset..source_end]);
			}
			last_end = source_end;
		}
	}

	// Add any trailing whitespace
	if last_end < statement.rql.len() {
		normalized.push_str(&statement.rql[last_end..]);
	}

	normalized
}
