// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Derive macros for ReifyDB.
//!
//! This crate provides the `#[derive(FromFrame)]` macro for ergonomic
//! deserialization of Frame data into Rust structs.
//!
//! # Example
//!
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

mod from_frame;
mod generate;
mod parse;

use proc_macro::TokenStream;

/// Derives `FromFrame` for a struct, enabling deserialization from a Frame.
///
/// # Attributes
///
/// - `#[frame(column = "name")]` - Use a different column name than the field name
/// - `#[frame(optional)]` - Field is optional; missing columns or Undefined values become None
/// - `#[frame(coerce)]` - Use widening type coercion for this field
/// - `#[frame(skip)]` - Skip this field (must implement Default)
#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	match parse::parse_struct(input) {
		Ok(parsed) => from_frame::expand(parsed),
		Err(err) => err,
	}
}
