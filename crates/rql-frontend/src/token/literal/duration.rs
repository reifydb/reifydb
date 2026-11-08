// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::token::{
	cursor::Cursor,
	identifier::is_identifier_char,
	token::{Literal, Token, TokenKind},
};

const UNITS: [&str; 9] = ["mo", "ms", "us", "ns", "y", "d", "h", "m", "s"];

pub fn scan_duration<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	if !cursor.peek().is_some_and(|c| c.is_ascii_digit()) {
		return None;
	}

	let state = cursor.save_state();
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	while cursor.peek().is_some_and(|c| c.is_ascii_digit()) {
		cursor.consume_while(|c| c.is_ascii_digit());

		let Some(unit) = UNITS.iter().find(|unit| cursor.peek_str(unit.len()) == **unit) else {
			cursor.restore_state(state);
			return None;
		};

		for _ in 0..unit.len() {
			cursor.consume();
		}
	}

	if cursor.peek().is_some_and(is_identifier_char) {
		cursor.restore_state(state);
		return None;
	}

	Some(Token {
		kind: TokenKind::Literal(Literal::Duration),
		fragment: cursor.make_fragment(start_pos, start_line, start_column),
	})
}
