// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use crate::ast::tokenize::token::{Token, TokenKind};

pub fn explain_tokenize(query: &str) -> crate::Result<String> {
	// Use the new tokenizer directly
	let tokens = crate::ast::tokenize::tokenize(query)?;

	let mut lines: BTreeMap<u32, Vec<(usize, &Token)>> = BTreeMap::new();
	for (i, token) in tokens.iter().enumerate() {
		lines.entry(token.fragment.line().0).or_default().push((i, token));
	}

	let mut result = String::new();

	for (line, tokens) in lines {
		result.push_str(&format!("Line {}:\n", line));
		for (i, token) in tokens {
			let label = match &token.kind {
				TokenKind::EOF => "EOF".to_string(),
				TokenKind::Identifier => format!("Identifier(\"{}\")", token.value()),
				TokenKind::Keyword(kw) => {
					format!("Keyword({:?})", kw)
				}
				TokenKind::Literal(lit) => {
					format!("Literal({:?})", lit)
				}
				TokenKind::Operator(op) => {
					format!("Operator({:?})", op)
				}
				TokenKind::Variable => {
					format!("Variable({:?})", token.value())
				}
				TokenKind::Separator(sep) => {
					format!("Separator({:?})", sep)
				}
			};

			result.push_str(&format!("  [{:>3}] {}\n", i, label));
		}
		result.push('\n');
	}

	Ok(result)
}
