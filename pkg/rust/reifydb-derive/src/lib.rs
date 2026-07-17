// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use proc_macro::TokenStream;
use reifydb_macro_impl::{derive_from_frame_with_crate, derive_heap_size as derive_heap_size_impl};

/// Derives `FromFrame` for a struct, enabling deserialization from a Frame.
///
/// Generated code references types from the `reifydb` crate.
///
/// # Attributes
///
/// - `#[frame(column = "name")]` - Use a different column name than the field name
/// - `#[frame(optional)]` - Field is optional; missing columns or None values become None
/// - `#[frame(coerce)]` - Use widening type coercion for this field
/// - `#[frame(skip)]` - Skip this field (must implement Default)
#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input.into(), "reifydb::value").into()
}

/// Derives `HeapSize` for a named-field struct by summing `heap_size()` over
/// every field.
///
/// The `HeapSize` trait must be in scope at the derive site. Importing
/// `HeapSize` from its defining module brings both the trait and this derive,
/// so a single `use` suffices.
#[proc_macro_derive(HeapSize)]
pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	derive_heap_size_impl(input.into()).into()
}
