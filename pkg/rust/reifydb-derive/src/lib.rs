// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
// This file is licensed under the MIT, see license.md file

//! Derive macros for ReifyDB that generate code using `reifydb` crate paths.
//!
//! This crate is re-exported by the `reifydb` crate, so users typically don't
//! need to depend on it directly.

use proc_macro::TokenStream;

/// Derives `FromFrame` for a struct, enabling deserialization from a Frame.
///
/// Generated code references types from the `reifydb` crate.
///
/// # Attributes
///
/// - `#[frame(column = "name")]` - Use a different column name than the field name
/// - `#[frame(optional)]` - Field is optional; missing columns or Undefined values become None
/// - `#[frame(coerce)]` - Use widening type coercion for this field
/// - `#[frame(skip)]` - Skip this field (must implement Default)
#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	reifydb_macro_impl::derive_from_frame_with_crate(input.into(), "reifydb").into()
}
