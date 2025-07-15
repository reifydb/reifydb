// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::Token;
use crate::ast::TokenKind::Literal;
use crate::ast::lex::Literal::Temporal;
use crate::ast::lex::as_span;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::char;
use nom::combinator::{complete, recognize};
use nom::sequence::{pair, preceded};
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

pub(crate) fn parse_temporal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let (rest, span) = preceded(
        char('@'),
        complete(recognize(alt((
            parse_range_interval,
            parse_duration_interval,
            parse_datetime,
            parse_date,
            parse_time,
        )))),
    )
    .parse(input)?;

    Ok((rest, Token { kind: Literal(Temporal), span: as_span(span) }))
}

fn parse_date(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, LocatedSpan<&str>> {
    let (rest, span) = recognize((
        take_while1(|c: char| c.is_ascii_digit()), // year
        char('-'),
        take_while1(|c: char| c.is_ascii_digit()), // month
        char('-'),
        take_while1(|c: char| c.is_ascii_digit()), // day
    ))
    .parse(input)?;

    // Verify we don't have trailing 'T' which would indicate an incomplete datetime
    if rest.fragment().starts_with('T') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }

    Ok((rest, span))
}

fn parse_time(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, LocatedSpan<&str>> {
    recognize((
        take_while1(|c: char| c.is_ascii_digit()), // hour
        char(':'),
        take_while1(|c: char| c.is_ascii_digit()), // minute
        char(':'),
        take_while1(|c: char| c.is_ascii_digit()), // second
    ))
    .parse(input)
}

fn parse_datetime(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, LocatedSpan<&str>> {
    recognize((
        take_while1(|c: char| c.is_ascii_digit()), // year
        char('-'),
        take_while1(|c: char| c.is_ascii_digit()), // month
        char('-'),
        take_while1(|c: char| c.is_ascii_digit()), // day
        char('T'),
        take_while1(|c: char| c.is_ascii_digit()), // hour
        char(':'),
        take_while1(|c: char| c.is_ascii_digit()), // minute
        char(':'),
        take_while1(|c: char| c.is_ascii_digit()), // second
    ))
    .parse(input)
}

fn parse_range_interval(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, LocatedSpan<&str>> {
    // First, try to parse a complete range
    let (rest, span) = recognize((
        alt((parse_datetime, parse_date, parse_time)),
        tag(".."),
        alt((parse_datetime, parse_date, parse_time)),
    ))
    .parse(input)?;

    // Verify that we have a complete range (both start and end)
    let fragment = span.fragment();
    if fragment.ends_with("..") || fragment.starts_with("..") {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }

    Ok((rest, span))
}

fn parse_duration_interval(
    input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, LocatedSpan<&str>> {
    use nom::multi::many1;

    recognize((
        char('P'),
        alt((
            // Date + time duration: P1DT1H
            recognize(pair(
                many1(alt((
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('Y'))), // years
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('M'))), // months
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('W'))), // weeks
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('D'))), // days
                ))),
                recognize(pair(
                    char('T'),
                    many1(alt((
                        recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('H'))), // hours
                        recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('M'))), // minutes
                        recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('S'))), // seconds
                    ))),
                )),
            )),
            // Time-only duration: PT1H
            recognize(pair(
                char('T'),
                many1(alt((
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('H'))), // hours
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('M'))), // minutes
                    recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('S'))), // seconds
                ))),
            )),
            // Date-only duration: P1D
            recognize(many1(alt((
                recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('Y'))), // years
                recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('M'))), // months
                recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('W'))), // weeks
                recognize(pair(take_while1(|c: char| c.is_ascii_digit()), char('D'))), // days
            )))),
        )),
    ))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::TokenKind::Literal;
    use crate::ast::lex::Literal::Temporal;
    use crate::ast::lex::literal::parse_literal;

    fn span(s: &str) -> LocatedSpan<&str> {
        LocatedSpan::new(s)
    }

    #[test]
    fn test_parse_date() {
        let cases = [
            ("@2024-03-15", true),
            ("@2024-12-31", true),
            ("@2000-01-01", true),
            ("@1999-02-28", true),
            ("@2024-3-15", true), // single digit month
            ("@2024-03-5", true), // single digit day
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_time() {
        let cases = [
            ("@14:30:00", true),
            ("@09:15:30", true),
            ("@23:59:59", true),
            ("@00:00:00", true),
            ("@1:2:3", true), // single digits
            ("@12:34:56", true),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_datetime() {
        let cases = [
            ("@2024-03-15T14:30:00", true),
            ("@2024-12-31T23:59:59", true),
            ("@2000-01-01T00:00:00", true),
            ("@1999-02-28T12:34:56", true),
            ("@2024-3-15T1:2:3", true), // single digits
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_range_interval_date() {
        let cases = [
            ("@2024-03-15..2024-03-16", true),
            ("@2024-01-01..2024-12-31", true),
            ("@2000-01-01..2025-01-01", true),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_range_interval_time() {
        let cases = [
            ("@14:30:00..15:30:00", true),
            ("@09:00:00..17:00:00", true),
            ("@00:00:00..23:59:59", true),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_range_interval_datetime() {
        let cases = [
            ("@2024-03-15T14:30:00..2024-03-15T15:30:00", true),
            ("@2024-01-01T00:00:00..2024-12-31T23:59:59", true),
            ("@2000-01-01T12:00:00..2025-01-01T12:00:00", true),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_duration_interval_date() {
        let cases = [
            ("@P1D", true),       // 1 day
            ("@P7D", true),       // 7 days
            ("@P1W", true),       // 1 week
            ("@P2W", true),       // 2 weeks
            ("@P1M", true),       // 1 month
            ("@P6M", true),       // 6 months
            ("@P1Y", true),       // 1 year
            ("@P2Y", true),       // 2 years
            ("@P1Y2M3W4D", true), // 1 year, 2 months, 3 weeks, 4 days
            ("@P365D", true),     // 365 days
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_duration_interval_time() {
        let cases = [
            ("@PT1H", true),       // 1 hour
            ("@PT30M", true),      // 30 minutes
            ("@PT45S", true),      // 45 seconds
            ("@PT2H30M", true),    // 2 hours 30 minutes
            ("@PT1H30M45S", true), // 1 hour 30 minutes 45 seconds
            ("@PT8H", true),       // 8 hours
            ("@PT90M", true),      // 90 minutes
            ("@PT3600S", true),    // 3600 seconds
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_duration_interval_datetime() {
        let cases = [
            ("@P1DT1H", true),         // 1 day 1 hour
            ("@P1Y2M3DT4H5M6S", true), // 1 year 2 months 3 days 4 hours 5 minutes 6 seconds
            ("@P7DT8H", true),         // 7 days 8 hours
            ("@P1MT2H", true),         // 1 month 2 hours
            ("@P365DT24H", true),      // 365 days 24 hours
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_invalid_temporal() {
        let cases = [
            ("@", false),                 // just @
            ("@invalid", false),          // invalid format
            ("@2024", false),             // incomplete date
            ("@2024-", false),            // incomplete date
            ("@2024-03", false),          // incomplete date
            ("@14:30", false),            // incomplete time
            ("@14:", false),              // incomplete time
            ("@2024-03-15T", false),      // incomplete datetime
            ("@2024-03-15T14", false),    // incomplete datetime
            ("@2024-03-15T14:30", false), // incomplete datetime
            ("@P", false),                // incomplete duration
            ("@PT", false),               // incomplete duration
            ("@..2024-03-15", false),     // incomplete range
            ("P1D", false),               // missing @
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_mixed_range_intervals() {
        let cases = [
            ("@2024-03-15..14:30:00", true),          // date to time
            ("@14:30:00..2024-03-15", true),          // time to date
            ("@2024-03-15T10:00:00..14:30:00", true), // datetime to time
            ("@14:30:00..2024-03-15T18:00:00", true), // time to datetime
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(span(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Temporal), "input = {}", input);
                    assert_eq!(token.value(), &input[1..], "input = {}", input); // skip @
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }
}
