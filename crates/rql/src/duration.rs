// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	error::{AstErrorKind, Error, TypeError},
	value::{duration::Duration, temporal::parse::duration::parse_duration},
};

use crate::token::token::{Literal, Token, TokenKind};

pub(crate) const FOREVER: &str = "forever";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurationBound {
	Positive,
	AllowZero,
}

pub(crate) fn compile_duration(token: &Token<'_>, bound: DurationBound, context: &str) -> Result<Duration> {
	if token.kind != TokenKind::Literal(Literal::Duration) {
		return Err(invalid_option(token, &format!("a bare duration literal such as `2h` for {}", context)));
	}

	let duration = parse_duration(token.fragment.to_owned())?;

	if duration.is_negative() || (bound == DurationBound::Positive && duration.is_zero()) {
		let expected = match bound {
			DurationBound::Positive => format!("a positive duration for {}", context),
			DurationBound::AllowZero => format!("a non-negative duration for {}", context),
		};
		return Err(invalid_option(token, &expected));
	}

	Ok(duration)
}

pub(crate) fn invalid_option(token: &Token<'_>, expected: &str) -> Error {
	Error::from(TypeError::Ast {
		kind: AstErrorKind::UnexpectedToken {
			expected: expected.to_string(),
		},
		message: format!("expected {}, found `{}`", expected, token.fragment.text()),
		fragment: token.fragment.to_owned(),
	})
}
