// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{cursor::Cursor, token::Token};

pub mod bool;
pub mod number;
pub mod temporal;
pub mod text;
pub mod undefined;

use bool::scan_boolean;
use number::scan_number;
use temporal::scan_temporal;
use text::scan_text;
use undefined::scan_undefined;

/// Scan for any literal token
pub fn scan_literal<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
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
