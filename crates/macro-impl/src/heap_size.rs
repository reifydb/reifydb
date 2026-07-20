// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::Peekable;

use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};

use crate::generate::compile_error;

pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	let tokens: Vec<TokenTree> = input.into_iter().collect();
	let mut iter = tokens.iter().peekable();

	while let Some(TokenTree::Punct(p)) = iter.peek() {
		if p.as_char() == '#' {
			iter.next();
			if let Some(TokenTree::Group(_)) = iter.peek() {
				iter.next();
			}
		} else {
			break;
		}
	}

	if let Some(TokenTree::Ident(i)) = iter.peek()
		&& *i == "pub"
	{
		iter.next();

		if let Some(TokenTree::Group(g)) = iter.peek()
			&& g.delimiter() == Delimiter::Parenthesis
		{
			iter.next();
		}
	}

	match iter.next() {
		Some(TokenTree::Ident(i)) if *i == "struct" => {}
		_ => return compile_error("HeapSize can only be derived for structs"),
	}

	let name = match iter.next() {
		Some(TokenTree::Ident(i)) => i.clone(),
		_ => return compile_error("expected struct name"),
	};

	let accessors = loop {
		match iter.next() {
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
				break named_field_accessors(g);
			}
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => {
				break tuple_field_accessors(g);
			}
			Some(TokenTree::Punct(p)) if p.as_char() == ';' => {
				break Vec::new();
			}
			Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
				let mut depth = 1;
				while depth > 0 {
					match iter.next() {
						Some(TokenTree::Punct(p)) if p.as_char() == '<' => depth += 1,
						Some(TokenTree::Punct(p)) if p.as_char() == '>' => depth -= 1,
						None => return compile_error("unclosed generic parameters"),
						_ => {}
					}
				}
			}
			Some(TokenTree::Ident(i)) if *i == "where" => {
				continue;
			}
			None => return compile_error("expected struct body"),
			_ => continue,
		}
	};

	let mut body = String::from("0usize");
	for accessor in &accessors {
		body.push_str(&format!(" + HeapSize::heap_size(&self.{})", accessor));
	}

	format!(
		"#[automatically_derived]\nimpl HeapSize for {} {{\n\tfn heap_size(&self) -> usize {{\n\t\t{}\n\t}}\n}}",
		name, body
	)
	.parse()
	.expect("derived HeapSize impl must be valid Rust")
}

fn named_field_accessors(group: &Group) -> Vec<String> {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	let mut iter = tokens.iter().peekable();
	let mut accessors = Vec::new();

	while iter.peek().is_some() {
		skip_field_attrs(&mut iter);
		skip_field_visibility(&mut iter);

		let name = match iter.next() {
			Some(TokenTree::Ident(i)) => i.to_string(),
			None => break,
			_ => continue,
		};

		match iter.next() {
			Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
			_ => continue,
		}

		skip_field_type(&mut iter);
		accessors.push(name);
	}

	accessors
}

fn tuple_field_accessors(group: &Group) -> Vec<String> {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	let mut iter = tokens.iter().peekable();
	let mut accessors = Vec::new();

	while iter.peek().is_some() {
		skip_field_attrs(&mut iter);
		skip_field_visibility(&mut iter);

		if iter.peek().is_none() {
			break;
		}

		if skip_field_type(&mut iter) {
			accessors.push(accessors.len().to_string());
		}
	}

	accessors
}

fn skip_field_attrs<'a>(iter: &mut Peekable<impl Iterator<Item = &'a TokenTree>>) {
	while let Some(TokenTree::Punct(p)) = iter.peek() {
		if p.as_char() == '#' {
			iter.next();
			if let Some(TokenTree::Group(_)) = iter.peek() {
				iter.next();
			}
		} else {
			break;
		}
	}
}

fn skip_field_visibility<'a>(iter: &mut Peekable<impl Iterator<Item = &'a TokenTree>>) {
	if let Some(TokenTree::Ident(i)) = iter.peek()
		&& *i == "pub"
	{
		iter.next();

		if let Some(TokenTree::Group(g)) = iter.peek()
			&& g.delimiter() == Delimiter::Parenthesis
		{
			iter.next();
		}
	}
}

fn skip_field_type<'a>(iter: &mut Peekable<impl Iterator<Item = &'a TokenTree>>) -> bool {
	let mut depth = 0;
	let mut consumed = false;

	loop {
		match iter.peek() {
			Some(TokenTree::Punct(p)) if p.as_char() == ',' && depth == 0 => {
				iter.next();
				break;
			}
			Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
				depth += 1;
				consumed = true;
				iter.next();
			}
			Some(TokenTree::Punct(p)) if p.as_char() == '>' => {
				depth -= 1;
				consumed = true;
				iter.next();
			}
			Some(_) => {
				consumed = true;
				iter.next();
			}
			None => break,
		}
	}

	consumed
}
