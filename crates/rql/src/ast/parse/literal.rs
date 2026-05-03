// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
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
	pub(crate) fn parse_literal(&mut self, kind: Literal) -> Result<AstLiteral<'bump>> {
		let token = self.consume_literal(kind)?;
		Ok(match kind {
			Literal::Number => AstLiteral::Number(AstLiteralNumber(token)),
			Literal::Text => AstLiteral::Text(AstLiteralText(token)),
			Literal::True | Literal::False => AstLiteral::Boolean(AstLiteralBoolean(token)),
			Literal::None => AstLiteral::None(AstLiteralNone(token)),
			Literal::Temporal => AstLiteral::Temporal(AstLiteralTemporal(token)),
		})
	}

	pub(crate) fn parse_literal_number(&mut self) -> Result<AstLiteral<'bump>> {
		self.parse_literal(Literal::Number)
	}

	pub(crate) fn parse_literal_text(&mut self) -> Result<AstLiteral<'bump>> {
		self.parse_literal(Literal::Text)
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
		let source = "'ElodiE'";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Text(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "ElodiE");
	}

	#[test]
	fn test_number_42() {
		let bump = Bump::new();
		let source = "42";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Number(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "42");
	}

	#[test]
	fn test_true() {
		let bump = Bump::new();
		let source = "true";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Boolean(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert!(node.value());
	}

	#[test]
	fn test_false() {
		let bump = Bump::new();
		let source = "false";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Boolean(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert!(!node.value());
	}

	#[test]
	fn test_date() {
		let bump = Bump::new();
		let source = "@2024-03-15";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15");
	}

	#[test]
	fn test_time() {
		let bump = Bump::new();
		let source = "@14:30:00";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00");
	}

	#[test]
	fn test_time_milliseconds() {
		let bump = Bump::new();
		let source = "@14:30:00.123";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123");
	}

	#[test]
	fn test_time_microseconds() {
		let bump = Bump::new();
		let source = "@14:30:00.123456";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456");
	}

	#[test]
	fn test_time_nanoseconds() {
		let bump = Bump::new();
		let source = "@14:30:00.123456789";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456789");
	}

	#[test]
	fn test_time_with_timezone() {
		let bump = Bump::new();
		let source = "@14:30:00Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00Z");
	}

	#[test]
	fn test_time_milliseconds_with_timezone() {
		let bump = Bump::new();
		let source = "@14:30:00.123Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123Z");
	}

	#[test]
	fn test_time_microseconds_with_timezone() {
		let bump = Bump::new();
		let source = "@14:30:00.123456Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456Z");
	}

	#[test]
	fn test_time_nanoseconds_with_timezone() {
		let bump = Bump::new();
		let source = "@14:30:00.123456789Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00.123456789Z");
	}

	#[test]
	fn test_time_with_offset_timezone() {
		let bump = Bump::new();
		let source = "@14:30:00+05:30";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00+05:30");
	}

	#[test]
	fn test_datetime() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00Z");
	}

	#[test]
	fn test_datetime_milliseconds() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00.123Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123Z");
	}

	#[test]
	fn test_datetime_microseconds() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00.123456Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456Z");
	}

	#[test]
	fn test_datetime_nanoseconds() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00.123456789Z";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456789Z");
	}

	#[test]
	fn test_datetime_without_timezone() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00");
	}

	#[test]
	fn test_datetime_milliseconds_without_timezone() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00.123";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123");
	}

	#[test]
	fn test_datetime_microseconds_without_timezone() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00.123456";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456");
	}

	#[test]
	fn test_datetime_nanoseconds_without_timezone() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00.123456789";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00.123456789");
	}

	#[test]
	fn test_range_interval_date() {
		let bump = Bump::new();
		let source = "@2024-03-15..2024-03-16";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15..2024-03-16");
	}

	#[test]
	fn test_range_interval_time() {
		let bump = Bump::new();
		let source = "@14:30:00..15:30:00";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "14:30:00..15:30:00");
	}

	#[test]
	fn test_range_interval_datetime() {
		let bump = Bump::new();
		let source = "@2024-03-15T14:30:00..2024-03-15T15:30:00";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15T14:30:00..2024-03-15T15:30:00");
	}

	#[test]
	fn test_mixed_range_interval() {
		let bump = Bump::new();
		let source = "@2024-03-15..14:30:00";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "2024-03-15..14:30:00");
	}

	#[test]
	fn test_duration_interval_date() {
		let bump = Bump::new();
		let source = "@P1D";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "P1D");
	}

	#[test]
	fn test_duration_interval_time() {
		let bump = Bump::new();
		let source = "@PT2H30M";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "PT2H30M");
	}

	#[test]
	fn test_duration_interval_datetime() {
		let bump = Bump::new();
		let source = "@P1Y2M3DT4H5M6S";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Temporal(node)) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(node.value(), "P1Y2M3DT4H5M6S");
	}
}
