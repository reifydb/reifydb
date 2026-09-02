// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod catalog_shape;
pub mod from_frame;
pub mod generate;
pub mod heap_size;
pub mod key;
pub mod operator_state;
pub mod parse;
pub mod typed_key;

use proc_macro2::TokenStream;

pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input, "reifydb_value")
}

pub fn expand_catalog_shape(input: TokenStream) -> TokenStream {
	catalog_shape::catalog_shape(input)
}

pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	heap_size::derive_heap_size(input)
}

pub fn derive_typed_key(input: TokenStream) -> TokenStream {
	typed_key::derive_typed_key(input)
}

pub fn derive_key(input: TokenStream) -> TokenStream {
	key::derive_key(input)
}

pub fn operator_state_with_crate(attr: TokenStream, item: TokenStream, crate_path: &str) -> TokenStream {
	operator_state::operator_state_impl(attr, item, crate_path)
}

pub fn derive_from_frame_with_crate(input: TokenStream, crate_path: &str) -> TokenStream {
	match parse::parse_struct_with_crate(input, crate_path) {
		Ok(parsed) => from_frame::expand(parsed),
		Err(err) => err,
	}
}
