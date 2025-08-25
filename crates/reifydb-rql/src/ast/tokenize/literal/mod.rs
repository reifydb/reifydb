// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::cursor::Cursor;
use crate::ast::lex::Token;

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
pub fn scan_literal(cursor: &mut Cursor) -> Option<Token> {
	// Try each literal type
	scan_text(cursor)
		.or_else(|| scan_number(cursor))
		.or_else(|| scan_boolean(cursor))
		.or_else(|| scan_undefined(cursor))
		.or_else(|| scan_temporal(cursor))
}
