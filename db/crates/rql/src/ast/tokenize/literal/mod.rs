// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{cursor::Cursor, token::Token};

mod bool;
mod number;
mod temporal;
mod text;
mod undefined;

pub use bool::scan_boolean;
pub use number::scan_number;
pub use temporal::scan_temporal;
pub use text::scan_text;
pub use undefined::scan_undefined;

/// Scan for any literal token
pub fn scan_literal<'a>(cursor: &mut Cursor<'a>) -> Option<Token<'a>> {
	// Try each literal type
	if let Some(token) = scan_text(cursor) {
		Some(token)
	} else if let Some(token) = scan_number(cursor) {
		Some(token)
	} else if let Some(token) = scan_boolean(cursor) {
		Some(token)
	} else if let Some(token) = scan_undefined(cursor) {
		Some(token)
	} else {
		scan_temporal(cursor)
	}
}
