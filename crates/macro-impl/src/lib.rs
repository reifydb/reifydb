// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod from_frame;
pub mod generate;
pub mod parse;

use proc_macro2::TokenStream;

/// Derive `FromFrame` with the default crate path (reifydb_type).
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input, "reifydb_type")
}

/// Derive `FromFrame` with a custom crate path.
///
/// # Arguments
/// * `input` - The derive macro input TokenStream
/// * `crate_path` - The crate path to use (e.g., "reifydb", "reifydb_client", "reifydb_type")
pub fn derive_from_frame_with_crate(input: TokenStream, crate_path: &str) -> TokenStream {
	match parse::parse_struct_with_crate(input, crate_path) {
		Ok(parsed) => from_frame::expand(parsed),
		Err(err) => err,
	}
}
