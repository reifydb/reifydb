// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use bumpalo::Bump;

use super::{LexError, LiteralKind, TokenKind, tokenize};

/// Explain tokenization by showing all tokens grouped by line.
///
/// # Arguments
///
/// * `source` - The RQL source code to tokenize
///
/// # Returns
///
/// A formatted string showing all tokens organized by line number,
/// or a `LexError` if tokenization fails.
pub fn explain_tokenize(source: &str) -> Result<String, LexError> {
	let bump = Bump::new();
	let result = tokenize(source, &bump)?;

	// Group tokens by line number
	let mut lines: BTreeMap<u32, Vec<(usize, &str)>> = BTreeMap::new();

	for (i, token) in result.tokens.iter().enumerate() {
		let text = result.text(token);
		let label = match &token.kind {
			TokenKind::Eof => "EOF".to_string(),
			TokenKind::Identifier => format!("Identifier(\"{}\")", text),
			TokenKind::QuotedIdentifier => format!("QuotedIdentifier(\"{}\")", text),
			TokenKind::Variable => format!("Variable(\"{}\")", text),
			TokenKind::Keyword(kw) => format!("Keyword({:?})", kw),
			TokenKind::Operator(op) => format!("Operator({:?})", op),
			TokenKind::Punctuation(p) => format!("Punctuation({:?})", p),
			TokenKind::Literal(lit) => match lit {
				LiteralKind::Integer => format!("Literal(Integer({}))", text),
				LiteralKind::Float => format!("Literal(Float({}))", text),
				LiteralKind::String => format!("Literal(String(\"{}\"))", text),
				LiteralKind::True => "Literal(True)".to_string(),
				LiteralKind::False => "Literal(False)".to_string(),
				LiteralKind::Undefined => "Literal(Undefined)".to_string(),
				LiteralKind::Temporal => format!("Literal(Temporal({}))", text),
			},
		};

		// Store the label with its index, grouped by line
		lines.entry(token.span.line).or_default().push((i, Box::leak(label.into_boxed_str())));
	}

	let mut output = String::new();

	for (line, tokens) in lines {
		output.push_str(&format!("Line {}:\n", line));
		for (i, label) in tokens {
			output.push_str(&format!("  [{:>3}] {}\n", i, label));
		}
		output.push('\n');
	}

	Ok(output)
}
