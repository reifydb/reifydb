// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};

use crate::generate::compile_error;

struct KeyField {
	name: String,
	ty: String,
	direction: &'static str,
	column: &'static str,
}

pub fn derive_key(input: TokenStream) -> TokenStream {
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
		_ => return compile_error("Key can only be derived for structs"),
	}

	let name = match iter.next() {
		Some(TokenTree::Ident(i)) => i.to_string(),
		_ => return compile_error("expected struct name"),
	};

	let body = match iter.next() {
		Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => g.clone(),
		Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
			return compile_error("Key cannot be derived for a generic struct");
		}
		Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => {
			return compile_error("Key requires named fields, so a tuple struct cannot carry a direction");
		}
		_ => return compile_error("expected struct body"),
	};

	let fields = match parse_fields(&body) {
		Ok(fields) => fields,
		Err(err) => return err,
	};

	expand(&name, &fields)
}

fn parse_fields(body: &Group) -> Result<Vec<KeyField>, TokenStream> {
	let tokens: Vec<TokenTree> = body.stream().into_iter().collect();
	let mut iter = tokens.iter().peekable();
	let mut fields = Vec::new();

	while iter.peek().is_some() {
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

		let field_name = match iter.next() {
			Some(TokenTree::Ident(i)) => i.to_string(),
			None => break,
			_ => return Err(compile_error("expected field name")),
		};

		match iter.next() {
			Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
			_ => return Err(compile_error("expected ':' after field name")),
		}

		let mut ty_tokens: Vec<TokenTree> = Vec::new();
		let mut depth = 0i32;
		loop {
			match iter.peek() {
				Some(TokenTree::Punct(p)) if p.as_char() == ',' && depth == 0 => {
					iter.next();
					break;
				}
				Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
					depth += 1;
					ty_tokens.push((*iter.next().unwrap()).clone());
				}
				Some(TokenTree::Punct(p)) if p.as_char() == '>' => {
					depth -= 1;
					ty_tokens.push((*iter.next().unwrap()).clone());
				}
				Some(t) => {
					ty_tokens.push((*t).clone());
					iter.next();
				}
				None => break,
			}
		}

		if ty_tokens.is_empty() {
			return Err(compile_error(&format!("field '{}' has no type", field_name)));
		}

		fields.push(field(field_name, &ty_tokens)?);
	}

	Ok(fields)
}

fn field(name: String, ty_tokens: &[TokenTree]) -> Result<KeyField, TokenStream> {
	let ty = render(ty_tokens);
	let unqualified = strip_path(ty_tokens);

	let direction = match unqualified.first() {
		Some(TokenTree::Ident(i)) if *i == "Asc" => "Asc",
		Some(TokenTree::Ident(i)) if *i == "Desc" => "Desc",
		_ => {
			return Err(compile_error(&format!(
				"field '{}' has type '{}': declare a direction by writing Asc<{}> or Desc<{}>",
				name, ty, ty, ty
			)));
		}
	};

	let opens = matches!(unqualified.get(1), Some(TokenTree::Punct(p)) if p.as_char() == '<');
	let closes = matches!(unqualified.last(), Some(TokenTree::Punct(p)) if p.as_char() == '>');
	if !opens || !closes || unqualified.len() < 4 {
		return Err(compile_error(&format!(
			"field '{}' has type '{}': expected a wrapped inner type",
			name, ty
		)));
	}

	let inner = &unqualified[2..unqualified.len() - 1];
	let column = match column_type(inner) {
		Some(column) => column,
		None => {
			return Err(compile_error(&format!(
				"field '{}' has inner type '{}', which has no key column type",
				name,
				render(inner)
			)));
		}
	};

	Ok(KeyField {
		name,
		ty,
		direction,
		column,
	})
}

fn column_type(inner: &[TokenTree]) -> Option<&'static str> {
	if let [TokenTree::Group(group)] = inner {
		return (group.delimiter() == Delimiter::Bracket && is_byte_array_16(group)).then_some("Blob16");
	}

	let unqualified = strip_path(inner);
	if unqualified.len() != 1 {
		return None;
	}

	let head = match unqualified.first() {
		Some(TokenTree::Ident(i)) => i.to_string(),
		_ => return None,
	};

	match head.as_str() {
		"u8" => Some("U8"),
		"u64" | "RowNumber" => Some("U64"),
		"GroupId" => Some("Blob16"),
		_ => None,
	}
}

fn is_byte_array_16(group: &Group) -> bool {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	match tokens.as_slice() {
		[TokenTree::Ident(element), TokenTree::Punct(semi), TokenTree::Literal(len)] => {
			*element == "u8" && semi.as_char() == ';' && len.to_string() == "16"
		}
		_ => false,
	}
}

fn strip_path(tokens: &[TokenTree]) -> &[TokenTree] {
	let mut at = 0;
	if is_colon(tokens.first()) && is_colon(tokens.get(1)) {
		at = 2;
	}
	while matches!(tokens.get(at), Some(TokenTree::Ident(_)))
		&& is_colon(tokens.get(at + 1))
		&& is_colon(tokens.get(at + 2))
	{
		at += 3;
	}
	&tokens[at..]
}

fn is_colon(token: Option<&TokenTree>) -> bool {
	matches!(token, Some(TokenTree::Punct(p)) if p.as_char() == ':')
}

fn render(tokens: &[TokenTree]) -> String {
	tokens.iter().map(|token| token.to_string()).collect()
}

fn expand(name: &str, fields: &[KeyField]) -> TokenStream {
	let mut cmp = String::from("::core::cmp::Ordering::Equal");
	for field in fields {
		cmp.push_str(&format!("\n\t\t\t.then_with(|| Ord::cmp(&self.{0}, &other.{0}))", field.name));
	}

	let mut columns = String::new();
	for field in fields {
		columns.push_str(&format!(
			"\n\t\tKeyColumn {{ name: \"{}\", ty: KeyColumnType::{}, direction: Direction::{} }},",
			field.name, field.column, field.direction
		));
	}

	let mut bounds = String::new();
	for field in fields {
		bounds.push_str(&format!("\n\t{}: KeyField,", field.ty));
	}

	let sealed = if fields.is_empty() {
		String::new()
	} else {
		format!("\n\n#[automatically_derived]\nimpl {} where{}\n{{\n}}", name, bounds)
	};

	let mut low = String::new();
	for field in fields {
		low.push_str(&format!("\n\t\t\t{}: Key::low(),", field.name));
	}

	let mut successor = String::new();
	for (at, field) in fields.iter().enumerate().rev() {
		successor.push_str(&format!("\t\tif let Some({0}) = Key::successor(&self.{0}) {{\n", field.name));
		successor.push_str("\t\t\treturn Some(Self {");
		for (other, carried) in fields.iter().enumerate() {
			let value = if other < at {
				format!("self.{}.clone()", carried.name)
			} else if other == at {
				carried.name.clone()
			} else {
				String::from("Key::low()")
			};
			successor.push_str(&format!("\n\t\t\t\t{}: {},", carried.name, value));
		}
		successor.push_str("\n\t\t\t});\n\t\t}\n\n");
	}
	successor.push_str("\t\tNone");

	let mut out = String::new();
	out.push_str(&format!("#[automatically_derived]\nimpl Ord for {name} {{\n"));
	out.push_str("\tfn cmp(&self, other: &Self) -> ::core::cmp::Ordering {\n");
	out.push_str(&format!("\t\t{cmp}\n\t}}\n}}\n\n"));
	out.push_str(&format!("#[automatically_derived]\nimpl PartialOrd for {name} {{\n"));
	out.push_str("\tfn partial_cmp(&self, other: &Self) -> Option<::core::cmp::Ordering> {\n");
	out.push_str("\t\tSome(Ord::cmp(self, other))\n\t}\n}\n\n");
	out.push_str(&format!("#[automatically_derived]\nimpl KeyLayout for {name} {{\n"));
	out.push_str(&format!("\tconst COLUMNS: &'static [KeyColumn] = &[{columns}\n\t];\n}}\n\n"));
	out.push_str(&format!("#[automatically_derived]\nimpl Key for {name} {{\n"));
	out.push_str(&format!("\tfn low() -> Self {{\n\t\tSelf {{{low}\n\t\t}}\n\t}}\n\n"));
	out.push_str("\tfn successor(&self) -> Option<Self> {\n");
	out.push_str(&format!("{successor}\n\t}}\n}}"));
	out.push_str(&sealed);

	out.parse().expect("derived Key impl must be valid Rust")
}

#[cfg(test)]
mod tests {
	use super::derive_key;

	fn expand(source: &str) -> String {
		derive_key(source.parse().unwrap()).to_string()
	}

	#[test]
	fn a_bare_field_is_rejected() {
		// a field with no wrapper would silently take Rust's ascending derived order, which inverts every
		// descending scan without failing anywhere
		let out = expand("struct JoinLeftKey { group: GroupId, row: Asc<RowNumber> }");
		assert!(out.contains("compile_error"), "{out}");
		assert!(out.contains("group"), "{out}");
		assert!(out.contains("declare a direction"), "{out}");
	}

	#[test]
	fn a_bare_field_in_trailing_position_is_rejected() {
		// the check must cover every field, not just the first one it looks at
		let out = expand("struct JoinLeftKey { group: Desc<GroupId>, row: RowNumber }");
		assert!(out.contains("compile_error"), "{out}");
		assert!(out.contains("row"), "{out}");
	}

	#[test]
	fn a_well_formed_struct_expands_without_an_error() {
		let out = expand("struct JoinLeftKey { group: Desc<GroupId>, row: Asc<RowNumber> }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("impl Ord for JoinLeftKey"), "{out}");
		assert!(out.contains("impl KeyLayout for JoinLeftKey"), "{out}");
	}

	#[test]
	fn columns_keep_the_declared_direction_in_field_order() {
		// COLUMNS drives the sqlite index declaration, so a reordered or flipped direction here is a
		// storage order that no longer matches the Rust Ord this same derive emitted
		let out = expand("struct JoinLeftKey { group: Desc<GroupId>, row: Asc<RowNumber> }");
		let desc = out.find("Direction :: Desc").expect("group must be descending");
		let asc = out.find("Direction :: Asc").expect("row must be ascending");
		assert!(desc < asc, "{out}");
		assert!(out.contains("KeyColumnType :: Blob16"), "{out}");
		assert!(out.contains("KeyColumnType :: U64"), "{out}");
	}

	#[test]
	fn column_names_are_the_field_names() {
		let out = expand("struct JoinLeftKey { group: Desc<GroupId>, row: Asc<RowNumber> }");
		let group = out.find("\"group\"").expect("group column");
		let row = out.find("\"row\"").expect("row column");
		assert!(group < row, "{out}");
	}

	#[test]
	fn every_field_carries_a_sealed_direction_bound() {
		// the syntactic check misses a type alias that hides a bare field, so the sealed bound has to be
		// emitted as well
		let out = expand("struct JoinLeftKey { group: Desc<GroupId>, row: Asc<RowNumber> }");
		assert!(out.contains("KeyField"), "{out}");
	}

	#[test]
	fn an_unrecognised_inner_type_is_rejected() {
		// guessing a column type would let a key claim a storage width it does not have
		let out = expand("struct WeirdKey { at: Asc<Instant> }");
		assert!(out.contains("compile_error"), "{out}");
		assert!(out.contains("at"), "{out}");
		assert!(out.contains("Instant"), "{out}");
	}

	#[test]
	fn a_path_qualified_wrapper_is_accepted() {
		let out = expand("struct JoinLeftKey { group: direction::Desc<GroupId> }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("Direction :: Desc"), "{out}");
	}

	#[test]
	fn a_row_number_field_maps_to_the_u64_column() {
		// RowNumber is a bounded eight byte newtype and is the suffix of five keyspaces, so it must be
		// nameable in a key rather than forcing the call sites back to a bare integer
		let out = expand("struct JoinLeftKey { row: Asc<RowNumber> }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("KeyColumnType :: U64"), "{out}");
	}

	#[test]
	fn a_byte_array_field_maps_to_the_blob_column() {
		let out = expand("struct CustomKey { blob: Asc<[u8; 16]> }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("KeyColumnType :: Blob16"), "{out}");
	}

	#[test]
	fn a_variable_width_field_is_rejected() {
		// an unbounded byte string has no maximum and no predecessor, so Desc of it cannot exist and no
		// key field may be variable width
		for source in [
			"struct CustomKey { blob: Asc<Bytes> }",
			"struct CustomKey { blob: Asc<Vec<u8>> }",
			"struct CustomKey { blob: Asc<EncodedKey> }",
		] {
			let out = expand(source);
			assert!(out.contains("compile_error"), "{out}");
			assert!(out.contains("blob"), "{out}");
		}
	}

	#[test]
	fn a_byte_array_of_the_wrong_width_is_rejected() {
		let out = expand("struct CustomKey { blob: Asc<[u8; 32]> }");
		assert!(out.contains("compile_error"), "{out}");
	}

	#[test]
	fn a_key_impl_is_emitted_with_the_carry_chain() {
		// successor must try the rightmost column first and reset everything to its right on carry,
		// otherwise a scan bound skips whole runs of the trailing column
		let out = expand("struct JoinLeftKey { group: Desc<GroupId>, row: Asc<RowNumber> }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("impl Key for JoinLeftKey"), "{out}");
		let row_first = out.find("Key :: successor (& self . row)").expect("row is tried first");
		let group_second = out.find("Key :: successor (& self . group)").expect("group carries");
		assert!(row_first < group_second, "{out}");
	}

	#[test]
	fn a_singleton_key_expands_to_an_empty_column_list() {
		// four keyspaces are singletons, so a key with no fields must stay legal
		let out = expand("struct NodeCounterKey {}");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("impl KeyLayout for NodeCounterKey"), "{out}");
	}

	#[test]
	fn a_tuple_struct_is_rejected() {
		// a positional field has no column name to emit
		let out = expand("struct JoinLeftKey(Desc<GroupId>);");
		assert!(out.contains("compile_error"), "{out}");
	}

	#[test]
	fn a_generic_struct_is_rejected() {
		// the generated impl header carries no parameters, so a generic key would not compile anyway
		let out = expand("struct JoinLeftKey<T> { group: Desc<T> }");
		assert!(out.contains("compile_error"), "{out}");
	}
}
