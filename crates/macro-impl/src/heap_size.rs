// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;

use crate::parse::parse_struct_with_crate;

pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	let parsed = match parse_struct_with_crate(input, "") {
		Ok(parsed) => parsed,
		Err(err) => return err,
	};

	let mut body = String::from("0usize");
	for field in &parsed.fields {
		body.push_str(&format!(" + HeapSize::heap_size(&self.{})", field.name));
	}

	format!(
		"#[automatically_derived]\nimpl HeapSize for {} {{\n\tfn heap_size(&self) -> usize {{\n\t\t{}\n\t}}\n}}",
		parsed.name, body
	)
	.parse()
	.expect("derived HeapSize impl must be valid Rust")
}
