// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{
			AstLiteral, AstLiteralBoolean, AstLiteralNone, AstLiteralNumber, AstLiteralTemporal,
			AstLiteralText,
		},
		parse::Parser,
	},
	token::token::Literal,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_literal_number(&mut self) -> crate::Result<AstLiteral<'bump>> {
		let token = self.consume_literal(Literal::Number)?;
		Ok(AstLiteral::Number(AstLiteralNumber(token)))
	}

	pub(crate) fn parse_literal_text(&mut self) -> crate::Result<AstLiteral<'bump>> {
		let token = self.consume_literal(Literal::Text)?;
		Ok(AstLiteral::Text(AstLiteralText(token)))
	}

	pub(crate) fn parse_literal_true(&mut self) -> crate::Result<AstLiteral<'bump>> {
		let token = self.consume_literal(Literal::True)?;
		Ok(AstLiteral::Boolean(AstLiteralBoolean(token)))
	}

	pub(crate) fn parse_literal_false(&mut self) -> crate::Result<AstLiteral<'bump>> {
		let token = self.consume_literal(Literal::False)?;
		Ok(AstLiteral::Boolean(AstLiteralBoolean(token)))
	}

	pub(crate) fn parse_literal_none(&mut self) -> crate::Result<AstLiteral<'bump>> {
		let token = self.consume_literal(Literal::None)?;
		Ok(AstLiteral::None(AstLiteralNone(token)))
	}

	pub(crate) fn parse_literal_temporal(&mut self) -> crate::Result<AstLiteral<'bump>> {
		let token = self.consume_literal(Literal::Temporal)?;
		Ok(AstLiteral::Temporal(AstLiteralTemporal(token)))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast::Literal, AstLiteral},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_text() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "'ElodiE'").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Text(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "ElodiE");
	}

	#[test]
	fn test_number_42() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "42").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Number(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "42");
	}

	#[test]
	fn test_true() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "true").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Boolean(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert!(node.value());
	}

	#[test]
	fn test_false() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "false").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Boolean(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert!(!node.value());
	}

	#[test]
	fn test_date() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15");
	}

	#[test]
	fn test_time() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00");
	}

	#[test]
	fn test_time_milliseconds() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00.123").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123");
	}

	#[test]
	fn test_time_microseconds() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00.123456").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456");
	}

	#[test]
	fn test_time_nanoseconds() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00.123456789").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456789");
	}

	#[test]
	fn test_time_with_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00Z");
	}

	#[test]
	fn test_time_milliseconds_with_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00.123Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123Z");
	}

	#[test]
	fn test_time_microseconds_with_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00.123456Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456Z");
	}

	#[test]
	fn test_time_nanoseconds_with_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00.123456789Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456789Z");
	}

	#[test]
	fn test_time_with_offset_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00+05:30").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00+05:30");
	}

	#[test]
	fn test_datetime() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00Z");
	}

	#[test]
	fn test_datetime_milliseconds() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00.123Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123Z");
	}

	#[test]
	fn test_datetime_microseconds() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00.123456Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456Z");
	}

	#[test]
	fn test_datetime_nanoseconds() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00.123456789Z").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456789Z");
	}

	#[test]
	fn test_datetime_without_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00");
	}

	#[test]
	fn test_datetime_milliseconds_without_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00.123").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123");
	}

	#[test]
	fn test_datetime_microseconds_without_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00.123456").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456");
	}

	#[test]
	fn test_datetime_nanoseconds_without_timezone() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15T14:30:00.123456789").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456789");
	}

	#[test]
	fn test_range_interval_date() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15..2024-03-16").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15..2024-03-16");
	}

	#[test]
	fn test_range_interval_time() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@14:30:00..15:30:00").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00..15:30:00");
	}

	#[test]
	fn test_range_interval_datetime() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "@2024-03-15T14:30:00..2024-03-15T15:30:00").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00..2024-03-15T15:30:00");
	}

	#[test]
	fn test_mixed_range_interval() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@2024-03-15..14:30:00").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15..14:30:00");
	}

	#[test]
	fn test_duration_interval_date() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@P1D").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "P1D");
	}

	#[test]
	fn test_duration_interval_time() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@PT2H30M").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "PT2H30M");
	}

	#[test]
	fn test_duration_interval_datetime() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "@P1Y2M3DT4H5M6S").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "P1Y2M3DT4H5M6S");
	}
}
