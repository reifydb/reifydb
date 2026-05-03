// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Implementation crate for ReifyDB derive macros. Holds the parsing, validation, and codegen for `FromFrame` (and
//! the family of related derives) so the public proc-macro crates - `reifydb-macro`, `reifydb-derive`, the future
//! `reifydb-client-derive` - can re-export a single shared expansion. Application code never depends on this crate
//! directly; depending on it creates a proc-macro dependency that the public crates already satisfy.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod from_frame;
pub mod generate;
pub mod parse;

use proc_macro2::TokenStream;

pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input, "reifydb_type")
}

pub fn derive_from_frame_with_crate(input: TokenStream, crate_path: &str) -> TokenStream {
	match parse::parse_struct_with_crate(input, crate_path) {
		Ok(parsed) => from_frame::expand(parsed),
		Err(err) => err,
	}
}
