// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Proc-macro backing `reifydb_testing::chaos_test!`. Expands one chaos workload into N separate `#[test]` functions
//! (`name_0 .. name_{N-1}`), one per iteration index, so they run and report independently under the test runner. N is
//! resolved at compile time: an explicit count argument pins the workload, otherwise the `CHAOS_ITERATIONS` environment
//! variable applies (falling back to 32). The expansion emits an `option_env!` reference so changing `CHAOS_ITERATIONS`
//! recompiles the dependent crate and this macro re-reads the new value. Codegen uses only the built-in `proc_macro`
//! crate (no external dependencies), matching the workspace's hand-rolled proc-macro style.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{env, str::FromStr};

use proc_macro::{Delimiter, TokenStream, TokenTree};

const DEFAULT_ITERATIONS: u64 = 32;

#[proc_macro]
pub fn chaos_test(input: TokenStream) -> TokenStream {
	match expand(input) {
		Ok(tokens) => tokens,
		Err(message) => compile_error(&message),
	}
}

fn expand(input: TokenStream) -> Result<TokenStream, String> {
	let tokens: Vec<TokenTree> = input.into_iter().collect();
	let mut pos = 0;

	let name = match tokens.get(pos) {
		Some(TokenTree::Ident(id)) => id.to_string(),
		_ => return Err("chaos_test! expects a test name identifier as the first argument".to_string()),
	};
	pos += 1;
	expect_punct(&tokens, &mut pos, ',')?;

	let count = match tokens.get(pos) {
		Some(TokenTree::Literal(lit)) => {
			let n = parse_count(&lit.to_string())?;
			pos += 1;
			expect_punct(&tokens, &mut pos, ',')?;
			n
		}
		_ => env_count()?,
	};

	expect_punct(&tokens, &mut pos, '|')?;
	let seed = match tokens.get(pos) {
		Some(TokenTree::Ident(id)) => id.to_string(),
		_ => return Err("chaos_test! expects a seed identifier inside the `|seed|` closure".to_string()),
	};
	pos += 1;
	expect_punct(&tokens, &mut pos, '|')?;

	let body = match tokens.get(pos) {
		Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => g.to_string(),
		_ => return Err("chaos_test! expects a `{ ... }` body block after the closure".to_string()),
	};
	pos += 1;

	if pos < tokens.len() {
		return Err("chaos_test! found unexpected tokens after the body block".to_string());
	}

	let mut out = format!("fn __chaos_body_{name}({seed}: u64) {body}\n");
	for index in 0..count {
		out.push_str(&generate_case(&name, index));
	}
	out.push_str("const _: ::core::option::Option<&'static str> = ::core::option_env!(\"CHAOS_ITERATIONS\");\n");

	TokenStream::from_str(&out).map_err(|e| format!("chaos_test! produced invalid tokens: {e:?}"))
}

fn generate_case(name: &str, index: u64) -> String {
	format!("#[test] fn {name}_{index}() {{ \
		 ::reifydb_testing::chaos::run_iteration({name:?}, {index}, __chaos_body_{name}); }}\n")
}

fn expect_punct(tokens: &[TokenTree], pos: &mut usize, ch: char) -> Result<(), String> {
	match tokens.get(*pos) {
		Some(TokenTree::Punct(p)) if p.as_char() == ch => {
			*pos += 1;
			Ok(())
		}
		_ => Err(format!("chaos_test! expected `{ch}`")),
	}
}

fn parse_count(literal: &str) -> Result<u64, String> {
	let n: u64 = literal.trim().parse().map_err(|_| {
		format!("chaos_test! iteration count must be a plain positive integer, got `{literal}`")
	})?;
	if n == 0 {
		return Err("chaos_test! iteration count must be >= 1".to_string());
	}
	Ok(n)
}

fn env_count() -> Result<u64, String> {
	match env::var("CHAOS_ITERATIONS") {
		Ok(raw) => parse_count(&raw),
		Err(_) => Ok(DEFAULT_ITERATIONS),
	}
}

fn compile_error(message: &str) -> TokenStream {
	TokenStream::from_str(&format!("::core::compile_error!({message:?});")).unwrap_or_else(|_| TokenStream::new())
}
