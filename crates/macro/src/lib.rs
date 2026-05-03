// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Public proc-macro front-end. Re-exports derives that expand to mappings between Rust structs and ReifyDB frames so
//! application code can read query results into typed values. The actual codegen lives in `reifydb-macro-impl`; this
//! crate is the thin entry point that the user adds to their `Cargo.toml`.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use proc_macro::TokenStream;
use reifydb_macro_impl::derive_from_frame_with_crate;

#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input.into(), "reifydb_type").into()
}
