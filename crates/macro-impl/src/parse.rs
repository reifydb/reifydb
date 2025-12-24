// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Token parsing for derive macro input.

use proc_macro2::{Delimiter, Group, Ident, TokenStream, TokenTree};

use crate::generate::compile_error;

/// Parsed struct information.
pub struct ParsedStruct {
	pub name: Ident,
	pub fields: Vec<ParsedField>,
	pub crate_path: String,
}

/// Parsed field information.
pub struct ParsedField {
	pub name: Ident,
	pub ty: Vec<TokenTree>,
	pub attrs: FieldAttrs,
}

/// Field attributes from #[frame(...)].
#[derive(Default)]
pub struct FieldAttrs {
	pub column_name: Option<String>,
	pub optional: bool,
	pub coerce: bool,
	pub skip: bool,
}

/// Parse a derive macro input into a ParsedStruct.
pub fn parse_struct(input: TokenStream) -> Result<ParsedStruct, TokenStream> {
	parse_struct_with_crate(input, "reifydb_type")
}

/// Parse a derive macro input into a ParsedStruct with a specific crate path.
pub fn parse_struct_with_crate(input: TokenStream, crate_path: &str) -> Result<ParsedStruct, TokenStream> {
	let tokens: Vec<TokenTree> = input.into_iter().collect();
	let mut iter = tokens.iter().peekable();
	let crate_path = crate_path.to_string();

	// Skip attributes on the struct itself
	while let Some(TokenTree::Punct(p)) = iter.peek() {
		if p.as_char() == '#' {
			iter.next(); // #
			if let Some(TokenTree::Group(_)) = iter.peek() {
				iter.next(); // [...]
			}
		} else {
			break;
		}
	}

	// Skip visibility (pub, pub(crate), etc.)
	if let Some(TokenTree::Ident(i)) = iter.peek()
		&& *i == "pub"
	{
		iter.next();
		// Handle pub(crate), pub(super), etc.
		if let Some(TokenTree::Group(g)) = iter.peek()
			&& g.delimiter() == Delimiter::Parenthesis
		{
			iter.next();
		}
	}

	// Expect "struct"
	match iter.next() {
		Some(TokenTree::Ident(i)) if *i == "struct" => {}
		_ => return Err(compile_error("FromFrame can only be derived for structs")),
	}

	// Get struct name
	let name = match iter.next() {
		Some(TokenTree::Ident(i)) => i.clone(),
		_ => return Err(compile_error("expected struct name")),
	};

	// Find the fields group (skip generics if present)
	let fields_group = loop {
		match iter.next() {
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => {
				break g.clone();
			}
			Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
				// Skip generics - for now we don't support generic structs
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
				// Skip where clause until we hit the brace
				continue;
			}
			None => return Err(compile_error("expected struct body")),
			_ => continue,
		}
	};

	// Parse fields
	let fields = parse_fields(fields_group)?;

	// Check for duplicate column aliases
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

/// Parse the fields from a struct body group.
fn parse_fields(group: Group) -> Result<Vec<ParsedField>, TokenStream> {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	let mut fields = Vec::new();
	let mut iter = tokens.iter().peekable();

	while iter.peek().is_some() {
		// Collect attributes
		let mut attrs_tokens = Vec::new();
		while let Some(TokenTree::Punct(p)) = iter.peek() {
			if p.as_char() == '#' {
				iter.next(); // #
				if let Some(TokenTree::Group(g)) = iter.next() {
					attrs_tokens.push(g.clone());
				}
			} else {
				break;
			}
		}

		// Skip visibility
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

		// Get field name
		let field_name = match iter.next() {
			Some(TokenTree::Ident(i)) => i.clone(),
			None => break, // End of fields
			_ => continue, // Skip unexpected tokens
		};

		// Expect colon
		match iter.next() {
			Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
			_ => return Err(compile_error("expected ':' after field name")),
		}

		// Collect type tokens until comma or end
		let mut ty_tokens = Vec::new();
		let mut depth = 0;
		loop {
			match iter.peek() {
				Some(TokenTree::Punct(p)) if p.as_char() == ',' && depth == 0 => {
					iter.next(); // consume comma
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

/// Parse #[frame(...)] attributes from a list of attribute groups.
fn parse_field_attrs(attr_groups: &[Group]) -> FieldAttrs {
	let mut result = FieldAttrs::default();

	for group in attr_groups {
		let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
		let mut iter = tokens.iter().peekable();

		// Check if this is a #[frame(...)] attribute
		match iter.next() {
			Some(TokenTree::Ident(i)) if *i == "frame" => {}
			_ => continue,
		}

		// Get the inner group
		let inner = match iter.next() {
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g,
			_ => continue,
		};

		// Parse inner tokens
		let inner_tokens: Vec<TokenTree> = inner.stream().into_iter().collect();
		let mut inner_iter = inner_tokens.iter().peekable();

		while inner_iter.peek().is_some() {
			// Get attribute name
			let attr_name = match inner_iter.next() {
				Some(TokenTree::Ident(i)) => i.to_string(),
				_ => continue,
			};

			match attr_name.as_str() {
				"column" => {
					// Expect = "value"
					if let Some(TokenTree::Punct(p)) = inner_iter.next()
						&& p.as_char() == '=' && let Some(TokenTree::Literal(lit)) =
						inner_iter.next()
					{
						let s = lit.to_string();
						// Remove quotes
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

			// Skip comma if present
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
	/// Get the column name, using the field name if not explicitly set.
	/// Strips r# prefix from raw identifiers.
	pub fn column_name(&self) -> String {
		if let Some(ref name) = self.attrs.column_name {
			name.clone()
		} else {
			self.safe_name()
		}
	}

	/// Get a safe variable name (strips r# prefix from raw identifiers).
	pub fn safe_name(&self) -> String {
		let name = self.name.to_string();
		name.strip_prefix("r#").unwrap_or(&name).to_string()
	}
}
