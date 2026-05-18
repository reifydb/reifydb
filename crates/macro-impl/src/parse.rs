// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use proc_macro2::{Delimiter, Group, Ident, TokenStream, TokenTree};

use crate::generate::compile_error;

pub struct ParsedStruct {
	pub name: Ident,
	pub fields: Vec<ParsedField>,
	pub crate_path: String,
}

pub struct ParsedField {
	pub name: Ident,
	pub ty: Vec<TokenTree>,
	pub attrs: FieldAttrs,
}

#[derive(Default)]
pub struct FieldAttrs {
	pub column_name: Option<String>,
	pub optional: bool,
	pub coerce: bool,
	pub skip: bool,
}

pub fn parse_struct(input: TokenStream) -> Result<ParsedStruct, TokenStream> {
	parse_struct_with_crate(input, "reifydb_type")
}

pub fn parse_struct_with_crate(input: TokenStream, crate_path: &str) -> Result<ParsedStruct, TokenStream> {
	let tokens: Vec<TokenTree> = input.into_iter().collect();
	let mut iter = tokens.iter().peekable();
	let crate_path = crate_path.to_string();

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
		_ => return Err(compile_error("FromFrame can only be derived for structs")),
	}

	let name = match iter.next() {
		Some(TokenTree::Ident(i)) => i.clone(),
		_ => return Err(compile_error("expected struct name")),
	};

	let fields_group = loop {
		match iter.next() {
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
				break g.clone();
			}
			Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
				let mut depth = 1;
				while depth > 0 {
					match iter.next() {
						Some(TokenTree::Punct(p)) if p.as_char() == '<' => depth += 1,
						Some(TokenTree::Punct(p)) if p.as_char() == '>' => depth -= 1,
						None => {
							return Err(compile_error("unclosed generic parameters"));
						}
						_ => {}
					}
				}
			}
			Some(TokenTree::Ident(i)) if *i == "where" => {
				continue;
			}
			None => return Err(compile_error("expected struct body")),
			_ => continue,
		}
	};

	let fields = parse_fields(fields_group)?;

	for (i, field) in fields.iter().enumerate() {
		if field.attrs.skip {
			continue;
		}
		let col_name = field.column_name();
		for other in fields.iter().skip(i + 1) {
			if other.attrs.skip {
				continue;
			}
			if other.column_name() == col_name {
				return Err(compile_error(&format!(
					"duplicate column alias '{}': used by both '{}' and '{}'",
					col_name,
					field.safe_name(),
					other.safe_name()
				)));
			}
		}
	}

	Ok(ParsedStruct {
		name,
		fields,
		crate_path,
	})
}

fn parse_fields(group: Group) -> Result<Vec<ParsedField>, TokenStream> {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	let mut fields = Vec::new();
	let mut iter = tokens.iter().peekable();

	while iter.peek().is_some() {
		let mut attrs_tokens = Vec::new();
		while let Some(TokenTree::Punct(p)) = iter.peek() {
			if p.as_char() == '#' {
				iter.next();
				if let Some(TokenTree::Group(g)) = iter.next() {
					attrs_tokens.push(g.clone());
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

		let field_name = match iter.next() {
			Some(TokenTree::Ident(i)) => i.clone(),
			None => break,
			_ => continue,
		};

		match iter.next() {
			Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
			_ => return Err(compile_error("expected ':' after field name")),
		}

		let mut ty_tokens = Vec::new();
		let mut depth = 0;
		loop {
			match iter.peek() {
				Some(TokenTree::Punct(p)) if p.as_char() == ',' && depth == 0 => {
					iter.next();
					break;
				}
				Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
					depth += 1;
					ty_tokens.push(iter.next().unwrap().clone());
				}
				Some(TokenTree::Punct(p)) if p.as_char() == '>' => {
					depth -= 1;
					ty_tokens.push(iter.next().unwrap().clone());
				}
				Some(t) => {
					ty_tokens.push((*t).clone());
					iter.next();
				}
				None => break,
			}
		}

		if ty_tokens.is_empty() {
			return Err(compile_error("expected field type"));
		}

		let attrs = parse_field_attrs(&attrs_tokens);

		fields.push(ParsedField {
			name: field_name,
			ty: ty_tokens,
			attrs,
		});
	}

	Ok(fields)
}

fn parse_field_attrs(attr_groups: &[Group]) -> FieldAttrs {
	let mut result = FieldAttrs::default();

	for group in attr_groups {
		let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
		let mut iter = tokens.iter().peekable();

		match iter.next() {
			Some(TokenTree::Ident(i)) if *i == "frame" => {}
			_ => continue,
		}

		let inner = match iter.next() {
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g,
			_ => continue,
		};

		let inner_tokens: Vec<TokenTree> = inner.stream().into_iter().collect();
		let mut inner_iter = inner_tokens.iter().peekable();

		while inner_iter.peek().is_some() {
			let attr_name = match inner_iter.next() {
				Some(TokenTree::Ident(i)) => i.to_string(),
				_ => continue,
			};

			match attr_name.as_str() {
				"column" => {
					if let Some(TokenTree::Punct(p)) = inner_iter.next()
						&& p.as_char() == '=' && let Some(TokenTree::Literal(lit)) =
						inner_iter.next()
					{
						let s = lit.to_string();

						if s.starts_with('"') && s.ends_with('"') {
							result.column_name = Some(s[1..s.len() - 1].to_string());
						}
					}
				}
				"optional" => result.optional = true,
				"coerce" => result.coerce = true,
				"skip" => result.skip = true,
				_ => {}
			}

			if let Some(TokenTree::Punct(p)) = inner_iter.peek()
				&& p.as_char() == ','
			{
				inner_iter.next();
			}
		}
	}

	result
}

impl ParsedField {
	pub fn column_name(&self) -> String {
		if let Some(ref name) = self.attrs.column_name {
			name.clone()
		} else {
			self.safe_name()
		}
	}

	pub fn safe_name(&self) -> String {
		let name = self.name.to_string();
		name.strip_prefix("r#").unwrap_or(&name).to_string()
	}
}
