// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{cursor::Cursor, token::Token};

pub mod bool;
pub mod none;
pub mod number;
pub mod temporal;
pub mod text;

use bool::scan_boolean;
use none::scan_none;
use number::scan_number;
use temporal::scan_temporal;
use text::scan_text;

pub fn scan_literal<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	if let Some(token) = scan_text(cursor) {
		Some(token)
	} else if let Some(token) = scan_number(cursor) {
		Some(token)
	} else if let Some(token) = scan_boolean(cursor) {
		Some(token)
	} else if let Some(token) = scan_none(cursor) {
		Some(token)
	} else {
		scan_temporal(cursor)
	}
}
