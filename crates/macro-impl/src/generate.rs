// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};

pub fn ident(name: &str) -> TokenTree {
	TokenTree::Ident(Ident::new(name, Span::call_site()))
}

pub fn underscore() -> TokenTree {
	"_".parse::<TokenStream>().unwrap().into_iter().next().unwrap()
}

#[allow(dead_code)]
pub fn ident_raw(name: &str) -> TokenTree {
	if let Some(stripped) = name.strip_prefix("r#") {
		TokenTree::Ident(Ident::new_raw(stripped, Span::call_site()))
	} else {
		TokenTree::Ident(Ident::new(name, Span::call_site()))
	}
}

pub fn punct(ch: char) -> TokenTree {
	TokenTree::Punct(Punct::new(ch, Spacing::Alone))
}

pub fn punct_joint(ch: char) -> TokenTree {
	TokenTree::Punct(Punct::new(ch, Spacing::Joint))
}

pub fn literal_str(s: &str) -> TokenTree {
	TokenTree::Literal(Literal::string(s))
}

pub fn literal_usize(n: usize) -> TokenTree {
	TokenTree::Literal(Literal::usize_unsuffixed(n))
}

pub fn group(delimiter: Delimiter, tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	TokenTree::Group(Group::new(delimiter, tokens.into_iter().collect()))
}

pub fn parens(tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	group(Delimiter::Parenthesis, tokens)
}

pub fn braces(tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	group(Delimiter::Brace, tokens)
}

pub fn brackets(tokens: impl IntoIterator<Item = TokenTree>) -> TokenTree {
	group(Delimiter::Bracket, tokens)
}

pub fn path_sep() -> impl Iterator<Item = TokenTree> {
	[punct_joint(':'), punct(':')].into_iter()
}

pub fn path(segments: &[&str]) -> Vec<TokenTree> {
	let mut tokens = Vec::new();
	let mut prev_was_empty = false;

	for (i, seg) in segments.iter().enumerate() {
		let need_sep = if i == 0 {
			seg.is_empty()
		} else {
			!prev_was_empty
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

pub fn arrow() -> impl Iterator<Item = TokenTree> {
	[punct_joint('-'), punct('>')].into_iter()
}

pub fn fat_arrow() -> impl Iterator<Item = TokenTree> {
	[punct_joint('='), punct('>')].into_iter()
}

#[allow(dead_code)]
pub fn generics(inner: impl IntoIterator<Item = TokenTree>) -> Vec<TokenTree> {
	let mut tokens = vec![punct('<')];
	tokens.extend(inner);
	tokens.push(punct('>'));
	tokens
}

#[allow(dead_code)]
pub fn extend(tokens: &mut Vec<TokenTree>, iter: impl IntoIterator<Item = TokenTree>) {
	tokens.extend(iter);
}

pub fn compile_error(message: &str) -> TokenStream {
	let tokens = vec![ident("compile_error"), punct('!'), parens([literal_str(message)])];
	tokens.into_iter().collect()
}
