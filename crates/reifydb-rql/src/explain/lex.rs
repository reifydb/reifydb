// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;

use crate::ast::{Token, TokenKind, lex::lex};

pub fn explain_lex(query: &str) -> crate::Result<String> {
	let tokens = lex(query).unwrap();

	let mut lines: BTreeMap<u32, Vec<(usize, &Token)>> = BTreeMap::new();
	for (i, token) in tokens.iter().enumerate() {
		lines.entry(token.fragment.line().0)
			.or_default()
			.push((i, token));
	}

	let mut result = String::new();

	for (line, tokens) in lines {
		result.push_str(&format!("Line {}:\n", line));
		for (i, token) in tokens {
			let label = match &token.kind {
				TokenKind::EOF => "EOF".to_string(),
				TokenKind::Identifier => format!(
					"Identifier(\"{}\")",
					token.value()
				),
				TokenKind::Keyword(kw) => {
					format!("Keyword({:?})", kw)
				}
				TokenKind::Literal(lit) => {
					format!("Literal({:?})", lit)
				}
				TokenKind::Operator(op) => {
					format!("Operator({:?})", op)
				}
				TokenKind::Parameter(param) => {
					format!("Parameter({:?})", param)
				}
				TokenKind::Separator(sep) => {
					format!("Separator({:?})", sep)
				}
			};

			result.push_str(&format!("  [{:>2}] {}\n", i, label));
		}
		result.push('\n');
	}

	Ok(result)
}
