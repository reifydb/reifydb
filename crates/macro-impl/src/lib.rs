// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Implementation for ReifyDB derive macros.
//!
//! This crate provides the implementation logic used by proc-macro crates.
//! It's not intended for direct use - use `reifydb-macro`, `reifydb-derive`,
//! or `reifydb-client-derive` instead.

#![cfg_attr(not(debug_assertions), deny(warnings))]

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
