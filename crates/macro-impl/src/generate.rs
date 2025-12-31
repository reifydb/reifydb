// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Token generation helpers for building TokenStreams without external dependencies.

use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};

/// Creates an identifier token.
pub fn ident(name: &str) -> TokenTree {
	TokenTree::Ident(Ident::new(name, Span::call_site()))
}

/// Creates an underscore token for type inference placeholders.
/// This is needed because Ident::new("_") panics in proc_macro v1.
pub fn underscore() -> TokenTree {
	"_".parse::<TokenStream>().unwrap().into_iter().next().unwrap()
}

/// Creates an identifier token, handling raw identifiers (r#keyword).
#[allow(dead_code)]
pub fn ident_raw(name: &str) -> TokenTree {
	if let Some(stripped) = name.strip_prefix("r#") {
		TokenTree::Ident(Ident::new_raw(stripped, Span::call_site()))
	} else {
		TokenTree::Ident(Ident::new(name, Span::call_site()))
	}
}

/// Creates a punctuation token with Alone spacing.
pub fn punct(ch: char) -> TokenTree {
	TokenTree::Punct(Punct::new(ch, Spacing::Alone))
}

/// Creates a punctuation token with Joint spacing (for multi-char punctuation like ::).
pub fn punct_joint(ch: char) -> TokenTree {
	TokenTree::Punct(Punct::new(ch, Spacing::Joint))
}

/// Creates a string literal token.
pub fn literal_str(s: &str) -> TokenTree {
	TokenTree::Literal(Literal::string(s))
}

/// Creates an integer literal token.
pub fn literal_usize(n: usize) -> TokenTree {
	TokenTree::Literal(Literal::usize_unsuffixed(n))
}

/// Creates a group (delimited tokens).
pub fn group(delimiter: Delimiter, tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	TokenTree::Group(Group::new(delimiter, tokens.into_iter().collect()))
}

/// Creates parentheses group: (tokens)
pub fn parens(tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	group(Delimiter::Parenthesis, tokens)
}

/// Creates brace group: {tokens}
pub fn braces(tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	group(Delimiter::Brace, tokens)
}

/// Creates bracket group: [tokens]
pub fn brackets(tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	group(Delimiter::Bracket, tokens)
}

/// Emits `::` (path separator).
pub fn path_sep() -> impl Iterator<Item = TokenTree> {
	[punct_joint(':'), punct(':')].into_iter()
}

/// Emits a path like `::reifydb_type::FromFrame`.
/// Pass an empty first segment for a global path (::foo::bar).
pub fn path(segments: &[&str]) -> Vec<TokenTree> {
	let mut tokens = Vec::new();
	let mut prev_was_empty = false;

	for (i, seg) in segments.iter().enumerate() {
		// Determine if we need to emit ::
		// - First segment: emit :: only if segment is empty (global path marker)
		// - Later segments: emit :: only if previous segment was NOT empty
		let need_sep = if i == 0 {
			seg.is_empty() // Leading :: for global paths
		} else {
			!prev_was_empty // Add :: only if prev was not the empty global marker
		};

		if need_sep {
			tokens.extend(path_sep());
		}

		if !seg.is_empty() {
			tokens.push(ident(seg));
		}

		prev_was_empty = seg.is_empty();
	}
	tokens
}

/// Emits `->` (return type arrow).
pub fn arrow() -> impl Iterator<Item = TokenTree> {
	[punct_joint('-'), punct('>')].into_iter()
}

/// Emits `=>` (match arm arrow).
pub fn fat_arrow() -> impl Iterator<Item = TokenTree> {
	[punct_joint('='), punct('>')].into_iter()
}

/// Emits `<` and `>` around tokens for generics.
#[allow(dead_code)]
pub fn generics(inner: impl IntoIterator<Item = TokenTree>) -> Vec<TokenTree> {
	let mut tokens = vec![punct('<')];
	tokens.extend(inner);
	tokens.push(punct('>'));
	tokens
}

/// Convenience: extends a Vec with an iterator.
#[allow(dead_code)]
pub fn extend(tokens: &mut Vec<TokenTree>, iter: impl IntoIterator<Item = TokenTree>) {
	tokens.extend(iter);
}

/// Creates a compile_error!("message") invocation.
pub fn compile_error(message: &str) -> TokenStream {
	let tokens = vec![ident("compile_error"), punct('!'), parens([literal_str(message)])];
	tokens.into_iter().collect()
}
