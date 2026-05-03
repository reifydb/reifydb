// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use proc_macro2::{TokenStream, TokenTree};

use crate::{
	generate::{
		arrow, braces, brackets, fat_arrow, ident, literal_str, literal_usize, parens, path, path_sep, punct,
		punct_joint, underscore,
	},
	parse::{ParsedField, ParsedStruct},
};

pub fn expand(parsed: ParsedStruct) -> TokenStream {
	let struct_name = parsed.name.to_string();
	let struct_name_lit = literal_str(&struct_name);
	let crate_path = &parsed.crate_path;

	let mut tokens = Vec::new();

	tokens.push(ident("impl"));
	tokens.extend(path(&["", crate_path, "value", "frame", "from_frame", "FromFrame"]));
	tokens.push(ident("for"));
	tokens.push(TokenTree::Ident(parsed.name.clone()));

	let impl_body = generate_from_frame_impl(&parsed.fields, &struct_name_lit, crate_path);
	tokens.push(braces(impl_body));

	tokens.into_iter().collect()
}

fn generate_from_frame_impl(fields: &[ParsedField], struct_name_lit: &TokenTree, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	tokens.push(ident("fn"));
	tokens.push(ident("from_frame"));
	tokens.push(parens([ident("frame"), punct(':'), punct('&')]
		.into_iter()
		.chain(path(&["", crate_path, "value", "frame", "frame", "Frame"]))));
	tokens.extend(arrow());
	tokens.push(ident("Result"));
	tokens.push(punct('<'));
	tokens.push(ident("Vec"));
	tokens.push(punct('<'));
	tokens.push(ident("Self"));
	tokens.push(punct('>'));
	tokens.push(punct(','));
	tokens.extend(path(&["", crate_path, "value", "frame", "from_frame", "FromFrameError"]));
	tokens.push(punct('>'));

	let mut body = Vec::new();

	for field in fields {
		if field.attrs.skip {
			continue;
		}
		body.extend(generate_column_lookup(field, struct_name_lit, crate_path));
	}

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

	for field in fields {
		if field.attrs.skip {
			continue;
		}
		body.extend(generate_field_extraction(field, crate_path));
	}

	body.push(ident("let"));
	body.push(ident("mut"));
	body.push(ident("result"));
	body.push(punct('='));
	body.push(ident("Vec"));
	body.extend(path_sep());
	body.push(ident("with_capacity"));
	body.push(parens([ident("row_count")]));
	body.push(punct(';'));

	body.push(ident("for"));
	body.push(ident("i"));
	body.push(ident("in"));
	body.push(literal_usize(0));
	body.extend([punct_joint('.'), punct('.')]);
	body.push(ident("row_count"));

	let mut loop_body = Vec::new();
	loop_body.push(ident("result"));
	loop_body.push(punct('.'));
	loop_body.push(ident("push"));

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

	body.push(ident("Ok"));
	body.push(parens([ident("result")]));

	tokens.push(braces(body));
	tokens
}

fn generate_column_lookup(field: &ParsedField, struct_name_lit: &TokenTree, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();
	let column_name = field.column_name();
	let col_var = format!("col_{}", field.safe_name());

	tokens.push(ident("let"));
	tokens.push(ident(&col_var));
	tokens.push(punct(':'));

	if field.attrs.optional {
		tokens.push(ident("Option"));
		tokens.push(punct('<'));
		tokens.push(punct('&'));
		tokens.extend(path(&["", crate_path, "value", "frame", "column", "FrameColumn"]));
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
		tokens.push(punct('&'));
		tokens.extend(path(&["", crate_path, "value", "frame", "column", "FrameColumn"]));
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

		let mut error_closure = Vec::new();
		error_closure.extend([punct('|'), punct('|')]);
		error_closure.extend(path(&[
			"",
			crate_path,
			"value",
			"frame",
			"from_frame",
			"FromFrameError",
			"MissingColumn",
		]));
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

fn generate_field_extraction(field: &ParsedField, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();
	let column_name = field.column_name();
	let col_var = format!("col_{}", field.safe_name());
	let values_var = format!("values_{}", field.safe_name());

	tokens.push(ident("let"));
	tokens.push(ident(&values_var));
	tokens.push(punct(':'));

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
		tokens.push(punct('='));

		tokens.push(ident("match"));
		tokens.push(ident(&col_var));

		let mut match_body = Vec::new();

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

		some_body.extend(path_sep());
		some_body.push(punct('<'));
		some_body.push(ident("Result"));
		some_body.push(punct('<'));
		some_body.push(underscore());
		some_body.push(punct(','));
		some_body.extend(path(&["", crate_path, "value", "frame", "from_frame", "FromFrameError"]));
		some_body.push(punct('>'));
		some_body.push(punct('>'));
		some_body.push(parens([]));
		some_body.push(punct('?'));

		match_body.push(braces(some_body));
		match_body.push(punct(','));

		match_body.push(ident("None"));
		match_body.extend(fat_arrow());
		match_body.push(ident("vec"));
		match_body.push(punct('!'));
		match_body.push(brackets([ident("None"), punct(';'), ident("row_count")]));
		match_body.push(punct(','));

		tokens.push(braces(match_body));
	} else {
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

		tokens.extend(path_sep());
		tokens.push(punct('<'));
		tokens.push(ident("Result"));
		tokens.push(punct('<'));
		tokens.push(underscore());
		tokens.push(punct(','));
		tokens.extend(path(&["", crate_path, "value", "frame", "from_frame", "FromFrameError"]));
		tokens.push(punct('>'));
		tokens.push(punct('>'));
		tokens.push(parens([]));
		tokens.push(punct('?'));
	}

	tokens.push(punct(';'));
	tokens
}

fn generate_optional_map_closure(
	trait_name: &str,
	method_name: &str,
	column_name: &str,
	crate_path: &str,
) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	tokens.push(punct('|'));
	tokens.push(parens([ident("row"), punct(','), ident("v")]));
	tokens.push(punct('|'));

	let mut body = vec![
		ident("if"),
		ident("matches"),
		punct('!'),
		parens([ident("v"), punct(',')]
			.into_iter()
			.chain(path(&["", crate_path, "value", "Value", "None"]))
			.chain([braces([punct_joint('.'), punct('.')])])),
	];

	body.push(braces([ident("Ok"), parens([ident("None")])]));

	body.push(ident("else"));

	let mut else_body = Vec::new();
	else_body.push(punct('<'));
	else_body.push(underscore());
	else_body.push(ident("as"));
	else_body.extend(path(&["", crate_path, "value", "try_from", trait_name]));
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

fn generate_required_map_closure(
	trait_name: &str,
	method_name: &str,
	column_name: &str,
	field_ty: &[TokenTree],
	crate_path: &str,
) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	tokens.push(punct('|'));
	tokens.push(parens([ident("row"), punct(','), ident("v")]));
	tokens.push(punct('|'));

	let mut body = Vec::new();
	body.push(punct('<'));
	body.extend(field_ty.iter().cloned());
	body.push(ident("as"));
	body.extend(path(&["", crate_path, "value", "try_from", trait_name]));
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

fn generate_error_closure(column_name: &str, crate_path: &str) -> Vec<TokenTree> {
	let mut tokens = Vec::new();

	tokens.push(punct('|'));
	tokens.push(ident("e"));
	tokens.push(punct('|'));
	tokens.extend(path(&["", crate_path, "value", "frame", "from_frame", "FromFrameError", "ValueError"]));
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
