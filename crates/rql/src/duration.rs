// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	value::{duration::Duration, temporal::parse::duration::parse_duration},
};

use crate::{
	diagnostic::AstError,
	token::token::{Literal, Token, TokenKind},
};

pub(crate) const FOREVER: &str = "forever";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurationBound {
	Positive,
	AllowZero,
}

pub(crate) fn compile_duration(token: &Token<'_>, bound: DurationBound, context: &str) -> Result<Duration> {
	if token.kind != TokenKind::Literal(Literal::Duration) {
		return Err(AstError::UnexpectedToken {
			expected: format!("a bare duration literal such as `2h` for {}", context),
			fragment: token.fragment.to_owned(),
		}
		.into());
	}

	let duration = parse_duration(token.fragment.to_owned())?;

	if duration.is_negative() || (bound == DurationBound::Positive && duration.is_zero()) {
		let expected = match bound {
			DurationBound::Positive => format!("a positive duration for {}", context),
			DurationBound::AllowZero => format!("a non-negative duration for {}", context),
		};
		return Err(AstError::UnexpectedToken {
			expected,
			fragment: token.fragment.to_owned(),
		}
		.into());
	}

	Ok(duration)
}
