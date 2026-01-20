// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Derive macros for ReifyDB.
//!
//! This crate provides the `#[derive(FromFrame)]` macro for ergonomic
//! deserialization of Frame data into Rust structs.
//!
//! # Example

// #![cfg_attr(not(debug_assertions), deny(warnings))]
//! ```ignore
//! use reifydb_type::FromFrame;
//!
//! #[derive(FromFrame)]
//! struct User {
//!     id: i64,
//!     name: String,
//!     #[frame(column = "created_at")]
//!     timestamp: i64,
//!     #[frame(optional)]
//!     email: Option<String>,
//! }
//!
//! let users: Vec<User> = frame.try_into()?;
//! ```

use proc_macro::TokenStream;

/// Derives `FromFrame` for a struct, enabling deserialization from a Frame.
///
/// This derive generates code that references types from `reifydb_type`.
/// For code that references `reifydb` or `reifydb_client`, use the
/// `FromFrame` derive from those crates instead.
///
/// # Attributes
///
/// - `#[frame(column = "name")]` - Use a different column name than the field name
/// - `#[frame(optional)]` - Field is optional; missing columns or Undefined values become None
/// - `#[frame(coerce)]` - Use widening type coercion for this field
/// - `#[frame(skip)]` - Skip this field (must implement Default)
#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	reifydb_macro_impl::derive_from_frame_with_crate(input.into(), "reifydb_type").into()
}
