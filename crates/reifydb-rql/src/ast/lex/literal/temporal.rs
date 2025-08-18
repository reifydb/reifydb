// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{
	IResult, Parser, bytes::complete::take_while1,
	character::complete::char, combinator::recognize, sequence::preceded,
};
use nom_locate::LocatedSpan;

use crate::ast::{
	Token,
	TokenKind::Literal,
	lex::{Literal::Temporal, as_fragment},
};

pub(crate) fn parse_temporal(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	let (rest, fragment) =
		preceded(char('@'), recognize(parse_temporal_content))
			.parse(input)?;

	Ok((
		rest,
		Token {
			kind: Literal(Temporal),
			fragment: as_fragment(fragment),
		},
	))
}

// Very permissive parser that accepts almost anything after @
// This allows the engine to provide better error diagnostics
fn parse_temporal_content(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, LocatedSpan<&str>> {
	// Accept any sequence of characters that could be part of a temporal
	// literal This includes letters, digits, colons, hyphens, dots, +, -,
	// etc.
	take_while1(|c: char| {
		c.is_ascii_alphanumeric()
			|| c == '-' || c == ':'
			|| c == '.' || c == '+'
			|| c == '/'
	})(input)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::{
		TokenKind::Literal,
		lex::{Literal::Temporal, literal::parse_literal},
	};

	fn fragment(s: &str) -> LocatedSpan<&str> {
		LocatedSpan::new(s)
	}

	#[test]
	fn test_parse_valid_temporal() {
		let cases = [
			// Valid dates
			("@2024-03-15", true),
			("@2024-12-31", true),
			("@2000-01-01", true),
			("@1999-02-28", true),
			("@2024-3-15", true), // single digit month
			("@2024-03-5", true), // single digit day
			// Valid times
			("@14:30:00", true),
			("@09:15:30", true),
			("@23:59:59", true),
			("@00:00:00", true),
			("@1:2:3", true), // single digits
			("@12:34:56", true),
			("@14:30:00.123", true),       // milliseconds
			("@14:30:00.123456", true),    // microseconds
			("@14:30:00.123456789", true), // nanoseconds
			("@14:30:00Z", true),          // with timezone
			("@14:30:00.123Z", true),      /* milliseconds with
			                                * timezone */
			("@14:30:00.123456Z", true), /* microseconds with
			                              * timezone */
			("@14:30:00.123456789Z", true), /* nanoseconds with
			                                 * timezone */
			("@14:30:00+05:30", true), // positive timezone offset
			("@14:30:00-08:00", true), // negative timezone offset
			// Valid datetimes
			("@2024-03-15T14:30:00", true),
			("@2024-12-31T23:59:59", true),
			("@2000-01-01T00:00:00", true),
			("@1999-02-28T12:34:56", true),
			("@2024-3-15T1:2:3", true),      // single digits
			("@2024-03-15T14:30:00Z", true), // with timezone
			("@2024-03-15T14:30:00.123Z", true), /* milliseconds
			                                  * with timezone */
			("@2024-03-15T14:30:00.123456Z", true), /* microseconds with timezone */
			("@2024-03-15T14:30:00.123456789Z", true), /* nanoseconds with timezone */
			("@2024-03-15T14:30:00.123", true),     /* milliseconds without timezone */
			("@2024-03-15T14:30:00.123456", true),  /* microseconds without timezone */
			("@2024-03-15T14:30:00.123456789", true), /* nanoseconds without timezone */
			("@2024-03-15T14:30:00+05:30", true),   /* positive timezone offset */
			("@2024-03-15T14:30:00-08:00", true),   /* negative timezone offset */
			// Valid intervals
			("@P1D", true), // 1 day
			("@P7D", true), // 7 days
			("@P1W", true), // 1 week
			("@P2W", true), // 2 weeks
			("@P1M", true), // 1 month
			("@P6M", true), // 6 months
			("@P1Y", true), // 1 year
			("@P2Y", true), // 2 years
			("@P1Y2M3W4D", true), /* 1 year, 2 months, 3
			                 * weeks, 4 days */
			("@P365D", true),   // 365 days
			("@PT1H", true),    // 1 hour
			("@PT30M", true),   // 30 minutes
			("@PT45S", true),   // 45 seconds
			("@PT2H30M", true), // 2 hours 30 minutes
			("@PT1H30M45S", true), /* 1 hour 30 minutes 45
			                     * seconds */
			("@PT8H", true),    // 8 hours
			("@PT90M", true),   // 90 minutes
			("@PT3600S", true), // 3600 seconds
			("@P1DT1H", true),  // 1 day 1 hour
			("@P1Y2M3DT4H5M6S", true), /* 1 year 2 months 3 days
			                     * 4 hours 5 minutes 6
			                     * seconds */
			("@P7DT8H", true),    // 7 days 8 hours
			("@P1MT2H", true),    // 1 month 2 hours
			("@P365DT24H", true), // 365 days 24 hours
			// Range intervals
			("@2024-03-15..2024-03-16", true),
			("@2024-01-01..2024-12-31", true),
			("@2000-01-01..2025-01-01", true),
			("@14:30:00..15:30:00", true),
			("@09:00:00..17:00:00", true),
			("@00:00:00..23:59:59", true),
			("@2024-03-15T14:30:00..2024-03-15T15:30:00", true),
			("@2024-01-01T00:00:00..2024-12-31T23:59:59", true),
			("@2000-01-01T12:00:00..2025-01-01T12:00:00", true),
			("@2024-03-15..14:30:00", true), // date to time
			("@14:30:00..2024-03-15", true), // time to date
			("@2024-03-15T10:00:00..14:30:00", true), // datetime to time
			("@14:30:00..2024-03-15T18:00:00", true), // time to datetime
			// Invalid patterns that should now be lexed (but will
			// fail in engine)
			("@invalid", true), /* invalid format - now
			                     * lexed */
			("@2024", true), /* incomplete date -
			                  * now lexed */
			("@2024-", true), /* incomplete date -
			                   * now lexed */
			("@2024-03", true), /* incomplete date -
			                     * now lexed */
			("@14:30", true), /* incomplete time -
			                   * now lexed */
			("@14:", true), /* incomplete time -
			                 * now lexed */
			("@2024-03-15T", true), /* incomplete datetime - now
			                         * lexed */
			("@2024-03-15T14", true), /* incomplete datetime -
			                           * now lexed */
			("@2024-03-15T14:30", true), /* incomplete datetime
			                              * - now lexed */
			("@P", true), /* incomplete duration - now
			               * lexed */
			("@PT", true),         /* incomplete duration - now
			                        * lexed */
			("@2024/03/15", true), // wrong format - now lexed
			("@25:99:99", true),   // invalid values - now lexed
			("@2024-13-45", true), // invalid values - now lexed
			("@1Y2M3D", true),     // missing P - now lexed
			("@PT2X", true),       // invalid character - now lexed
			("@abcd-03-15", true), // invalid year - now lexed
		];

		for (input, should_parse) in cases {
			let result = parse_literal(fragment(input));
			match (result, should_parse) {
				(Ok((_rest, token)), true) => {
					assert_eq!(
						token.kind,
						Literal(Temporal),
						"input = {}",
						input
					);
					assert_eq!(
						token.value(),
						&input[1..],
						"input = {}",
						input
					); // skip @
				}
				(Err(_), false) => {}
				(Ok(_), false) => panic!(
					"input {:?} should NOT parse but did",
					input
				),
				(Err(e), true) => panic!(
					"input {:?} should parse but failed: {:?}",
					input, e
				),
			}
		}
	}

	#[test]
	fn test_parse_invalid_temporal() {
		let cases = [
			("@", false),   // just @, no content
			("P1D", false), // missing @
		];

		for (input, should_parse) in cases {
			let result = parse_literal(fragment(input));
			match (result, should_parse) {
				(Ok((_rest, token)), true) => {
					assert_eq!(
						token.kind,
						Literal(Temporal),
						"input = {}",
						input
					);
					assert_eq!(
						token.value(),
						&input[1..],
						"input = {}",
						input
					); // skip @
				}
				(Err(_), false) => {}
				(Ok(_), false) => panic!(
					"input {:?} should NOT parse but did",
					input
				),
				(Err(e), true) => panic!(
					"input {:?} should parse but failed: {:?}",
					input, e
				),
			}
		}
	}
}
