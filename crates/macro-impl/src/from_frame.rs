// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Code generation for the FromFrame derive macro.

use proc_macro2::{TokenStream, TokenTree};

use crate::{
	generate::{
		arrow, braces, brackets, fat_arrow, ident, literal_str, literal_usize, parens, path, path_sep, punct,
		punct_joint, underscore,
	},
	parse::{ParsedField, ParsedStruct},
};

/// Expand a parsed struct into the FromFrame implementation.
pub fn expand(parsed: ParsedStruct) -> TokenStream {
	let struct_name = parsed.name.to_string();
	let struct_name_lit = literal_str(&struct_name);
	let crate_path = &parsed.crate_path;

	let mut tokens = Vec::new();

	// impl ::crate_path::FromFrame for StructName
	tokens.push(ident("impl"));
	tokens.extend(path(&["", crate_path, "FromFrame"]));
	tokens.push(ident("for"));
	tokens.push(TokenTree::Ident(parsed.name.clone()));

	// Implementation body
	let impl_body = generate_from_frame_impl(&parsed.fields, &struct_name_lit, crate_path);
	tokens.push(braces(impl_body));

	// Note: TryFrom<&Frame> impl is NOT generated because it would violate orphan rules
	// when the derive is used outside reifydb-type. Use FromFrame::from_frame() instead.

	tokens.into_iter().collect()
}

/// Generate the FromFrame::from_frame method body.
fn generate_from_frame_impl(fields: &[ParsedField], struct_name_lit: &TokenTree, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	// fn from_frame(frame: &::crate_path::Frame) -> Result<Vec<Self>, ::crate_path::FromFrameError>
	tokens.push(ident("fn"));
	tokens.push(ident("from_frame"));
	tokens.push(parens([ident("frame"), punct(':'), punct('&')]
		.into_iter()
		.chain(path(&["", crate_path, "Frame"]))));
	tokens.extend(arrow());
	tokens.push(ident("Result"));
	tokens.push(punct('<'));
	tokens.push(ident("Vec"));
	tokens.push(punct('<'));
	tokens.push(ident("Self"));
	tokens.push(punct('>'));
	tokens.push(punct(','));
	tokens.extend(path(&["", crate_path, "FromFrameError"]));
	tokens.push(punct('>'));

	// Method body
	let mut body = Vec::new();

	// Column lookups
	for field in fields {
		if field.attrs.skip {
			continue;
		}
		body.extend(generate_column_lookup(field, struct_name_lit, crate_path));
	}

	// Row count
	body.push(ident("let"));
	body.push(ident("row_count"));
	body.push(punct('='));
	body.push(ident("frame"));
	body.push(punct('.'));
	body.push(ident("columns"));
	body.push(punct('.'));
	body.push(ident("first"));
	body.push(parens([]));
	body.push(punct('.'));
	body.push(ident("map"));
	body.push(parens([
		punct('|'),
		ident("c"),
		punct('|'),
		ident("c"),
		punct('.'),
		ident("data"),
		punct('.'),
		ident("len"),
		parens([]),
	]));
	body.push(punct('.'));
	body.push(ident("unwrap_or"));
	body.push(parens([literal_usize(0)]));
	body.push(punct(';'));

	// Field extractions
	for field in fields {
		if field.attrs.skip {
			continue;
		}
		body.extend(generate_field_extraction(field, crate_path));
	}

	// Result construction
	body.push(ident("let"));
	body.push(ident("mut"));
	body.push(ident("result"));
	body.push(punct('='));
	body.push(ident("Vec"));
	body.extend(path_sep());
	body.push(ident("with_capacity"));
	body.push(parens([ident("row_count")]));
	body.push(punct(';'));

	// for loop
	body.push(ident("for"));
	body.push(ident("i"));
	body.push(ident("in"));
	body.push(literal_usize(0));
	body.extend([punct_joint('.'), punct('.')]);
	body.push(ident("row_count"));

	// Loop body
	let mut loop_body = Vec::new();
	loop_body.push(ident("result"));
	loop_body.push(punct('.'));
	loop_body.push(ident("push"));

	// Self { field: values_field[i].clone(), ... }
	let mut constructor = vec![ident("Self")];
	let mut field_inits = Vec::new();
	for (i, field) in fields.iter().enumerate() {
		if i > 0 {
			field_inits.push(punct(','));
		}
		field_inits.push(TokenTree::Ident(field.name.clone()));
		field_inits.push(punct(':'));
		if field.attrs.skip {
			field_inits.extend(path(&["", "std", "default", "Default", "default"]));
			field_inits.push(parens([]));
		} else {
			let values_var = format!("values_{}", field.safe_name());
			field_inits.push(ident(&values_var));
			field_inits.push(brackets([ident("i")]));
			field_inits.push(punct('.'));
			field_inits.push(ident("clone"));
			field_inits.push(parens([]));
		}
	}
	constructor.push(braces(field_inits));
	loop_body.push(parens(constructor));
	loop_body.push(punct(';'));

	body.push(braces(loop_body));

	// Ok(result)
	body.push(ident("Ok"));
	body.push(parens([ident("result")]));

	tokens.push(braces(body));
	tokens
}

/// Generate column lookup for a field.
fn generate_column_lookup(field: &ParsedField, struct_name_lit: &TokenTree, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();
	let column_name = field.column_name();
	let col_var = format!("col_{}", field.safe_name());

	tokens.push(ident("let"));
	tokens.push(ident(&col_var));
	tokens.push(punct(':'));

	if field.attrs.optional {
		// Option<&::crate_path::FrameColumn>
		tokens.push(ident("Option"));
		tokens.push(punct('<'));
		tokens.push(punct('&'));
		tokens.extend(path(&["", crate_path, "FrameColumn"]));
		tokens.push(punct('>'));
		tokens.push(punct('='));
		tokens.push(ident("frame"));
		tokens.push(punct('.'));
		tokens.push(ident("columns"));
		tokens.push(punct('.'));
		tokens.push(ident("iter"));
		tokens.push(parens([]));
		tokens.push(punct('.'));
		tokens.push(ident("find"));
		tokens.push(parens([
			punct('|'),
			ident("c"),
			punct('|'),
			ident("c"),
			punct('.'),
			ident("name"),
			punct_joint('='),
			punct('='),
			literal_str(&column_name),
		]));
	} else {
		// &::crate_path::FrameColumn
		tokens.push(punct('&'));
		tokens.extend(path(&["", crate_path, "FrameColumn"]));
		tokens.push(punct('='));
		tokens.push(ident("frame"));
		tokens.push(punct('.'));
		tokens.push(ident("columns"));
		tokens.push(punct('.'));
		tokens.push(ident("iter"));
		tokens.push(parens([]));
		tokens.push(punct('.'));
		tokens.push(ident("find"));
		tokens.push(parens([
			punct('|'),
			ident("c"),
			punct('|'),
			ident("c"),
			punct('.'),
			ident("name"),
			punct_joint('='),
			punct('='),
			literal_str(&column_name),
		]));
		tokens.push(punct('.'));
		tokens.push(ident("ok_or_else"));

		// || ::crate_path::FromFrameError::MissingColumn { ... }
		let mut error_closure = Vec::new();
		error_closure.extend([punct('|'), punct('|')]);
		error_closure.extend(path(&["", crate_path, "FromFrameError", "MissingColumn"]));
		error_closure.push(braces([
			ident("column"),
			punct(':'),
			literal_str(&column_name),
			punct('.'),
			ident("to_string"),
			parens([]),
			punct(','),
			ident("struct_name"),
			punct(':'),
			struct_name_lit.clone(),
			punct(','),
		]));
		tokens.push(parens(error_closure));
		tokens.push(punct('?'));
	}

	tokens.push(punct(';'));
	tokens
}

/// Generate field extraction for a field.
fn generate_field_extraction(field: &ParsedField, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();
	let column_name = field.column_name();
	let col_var = format!("col_{}", field.safe_name());
	let values_var = format!("values_{}", field.safe_name());

	tokens.push(ident("let"));
	tokens.push(ident(&values_var));
	tokens.push(punct(':'));

	// Type: Vec<FieldType>
	tokens.push(ident("Vec"));
	tokens.push(punct('<'));
	tokens.extend(field.ty.iter().cloned());
	tokens.push(punct('>'));

	let trait_name = if field.attrs.coerce {
		"TryFromValueCoerce"
	} else {
		"TryFromValue"
	};

	let method_name = if field.attrs.coerce {
		"try_from_value_coerce"
	} else {
		"try_from_value"
	};

	if field.attrs.optional {
		// let values_x = match col_x { Some(col) => ..., None => vec![None; row_count] };
		tokens.push(punct('='));

		// match col_var { Some(col) => ..., None => vec![None; row_count] }
		tokens.push(ident("match"));
		tokens.push(ident(&col_var));

		let mut match_body = Vec::new();

		// Some(col) => { ... }
		match_body.push(ident("Some"));
		match_body.push(parens([ident("col")]));
		match_body.extend(fat_arrow());

		let mut some_body = vec![
			ident("col"),
			punct('.'),
			ident("data"),
			punct('.'),
			ident("iter"),
			parens([]),
			punct('.'),
			ident("enumerate"),
			parens([]),
			punct('.'),
			ident("map"),
		];
		some_body.push(parens(generate_optional_map_closure(
			trait_name,
			method_name,
			&column_name,
			crate_path,
		)));
		some_body.push(punct('.'));
		some_body.push(ident("collect"));
		// ::<Result<_, ::crate_path::FromFrameError>>
		some_body.extend(path_sep());
		some_body.push(punct('<'));
		some_body.push(ident("Result"));
		some_body.push(punct('<'));
		some_body.push(underscore());
		some_body.push(punct(','));
		some_body.extend(path(&["", crate_path, "FromFrameError"]));
		some_body.push(punct('>'));
		some_body.push(punct('>'));
		some_body.push(parens([]));
		some_body.push(punct('?'));

		match_body.push(braces(some_body));
		match_body.push(punct(','));

		// None => vec![None; row_count]
		match_body.push(ident("None"));
		match_body.extend(fat_arrow());
		match_body.push(ident("vec"));
		match_body.push(punct('!'));
		match_body.push(brackets([ident("None"), punct(';'), ident("row_count")]));
		match_body.push(punct(','));

		tokens.push(braces(match_body));
	} else {
		// let values_x = col_x.data.iter()...collect::<Result<_, FromFrameError>>()?;
		tokens.push(punct('='));

		tokens.push(ident(&col_var));
		tokens.push(punct('.'));
		tokens.push(ident("data"));
		tokens.push(punct('.'));
		tokens.push(ident("iter"));
		tokens.push(parens([]));
		tokens.push(punct('.'));
		tokens.push(ident("enumerate"));
		tokens.push(parens([]));
		tokens.push(punct('.'));
		tokens.push(ident("map"));
		tokens.push(parens(generate_required_map_closure(
			trait_name,
			method_name,
			&column_name,
			&field.ty,
			crate_path,
		)));
		tokens.push(punct('.'));
		tokens.push(ident("collect"));
		// ::<Result<_, ::crate_path::FromFrameError>>
		tokens.extend(path_sep());
		tokens.push(punct('<'));
		tokens.push(ident("Result"));
		tokens.push(punct('<'));
		tokens.push(underscore());
		tokens.push(punct(','));
		tokens.extend(path(&["", crate_path, "FromFrameError"]));
		tokens.push(punct('>'));
		tokens.push(punct('>'));
		tokens.push(parens([]));
		tokens.push(punct('?'));
	}

	tokens.push(punct(';'));
	tokens
}

/// Generate the map closure for optional fields.
fn generate_optional_map_closure(
	trait_name: &str,
	method_name: &str,
	column_name: &str,
	crate_path: &str,
) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	// |(row, v)| { if matches!(v, ...) { Ok(None) } else { ... } }
	tokens.push(punct('|'));
	tokens.push(parens([ident("row"), punct(','), ident("v")]));
	tokens.push(punct('|'));

	// if matches!(v, ::crate_path::Value::Undefined)
	let mut body = vec![
		ident("if"),
		ident("matches"),
		punct('!'),
		parens([ident("v"), punct(',')].into_iter().chain(path(&["", crate_path, "Value", "Undefined"]))),
	];

	// { Ok(None) }
	body.push(braces([ident("Ok"), parens([ident("None")])]));

	// else { <_ as Trait>::method(&v).map(Some).map_err(...) }
	body.push(ident("else"));

	let mut else_body = Vec::new();
	else_body.push(punct('<'));
	else_body.push(underscore());
	else_body.push(ident("as"));
	else_body.extend(path(&["", crate_path, trait_name]));
	else_body.push(punct('>'));
	else_body.extend(path_sep());
	else_body.push(ident(method_name));
	else_body.push(parens([punct('&'), ident("v")]));
	else_body.push(punct('.'));
	else_body.push(ident("map"));
	else_body.push(parens([ident("Some")]));
	else_body.push(punct('.'));
	else_body.push(ident("map_err"));
	else_body.push(parens(generate_error_closure(column_name, crate_path)));

	body.push(braces(else_body));

	tokens.push(braces(body));
	tokens
}

/// Generate the map closure for required fields.
fn generate_required_map_closure(
	trait_name: &str,
	method_name: &str,
	column_name: &str,
	field_ty: &[TokenTree],
	crate_path: &str,
) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	// |(row, v)| { <Type as Trait>::method(&v).map_err(...) }
	tokens.push(punct('|'));
	tokens.push(parens([ident("row"), punct(','), ident("v")]));
	tokens.push(punct('|'));

	let mut body = Vec::new();
	body.push(punct('<'));
	body.extend(field_ty.iter().cloned());
	body.push(ident("as"));
	body.extend(path(&["", crate_path, trait_name]));
	body.push(punct('>'));
	body.extend(path_sep());
	body.push(ident(method_name));
	body.push(parens([punct('&'), ident("v")]));
	body.push(punct('.'));
	body.push(ident("map_err"));
	body.push(parens(generate_error_closure(column_name, crate_path)));

	tokens.push(braces(body));
	tokens
}

/// Generate the error closure for map_err.
fn generate_error_closure(column_name: &str, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	// |e| ::crate_path::FromFrameError::ValueError { column: "...", row, error: e }
	tokens.push(punct('|'));
	tokens.push(ident("e"));
	tokens.push(punct('|'));
	tokens.extend(path(&["", crate_path, "FromFrameError", "ValueError"]));
	tokens.push(braces([
		ident("column"),
		punct(':'),
		literal_str(column_name),
		punct('.'),
		ident("to_string"),
		parens([]),
		punct(','),
		ident("row"),
		punct(','),
		ident("error"),
		punct(':'),
		ident("e"),
		punct(','),
	]));

	tokens
}
