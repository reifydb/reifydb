// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use proc_macro::TokenStream;
use reifydb_macro_impl::{derive_from_frame_with_crate, derive_heap_size as derive_heap_size_impl};

/// Field attributes: `#[frame(column = "name")]` overrides the column name, `#[frame(optional)]`
/// maps a missing column or a none value to `None`, `#[frame(coerce)]` widens the value type,
/// `#[frame(skip)]` falls back to `Default`. Generated code names types from the `reifydb` crate.
#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input.into(), "reifydb::value").into()
}

/// Named-field structs only. The `HeapSize` trait must be in scope at the derive site; importing it
/// from its defining module brings both the trait and this derive.
#[proc_macro_derive(HeapSize)]
pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	derive_heap_size_impl(input.into()).into()
}
