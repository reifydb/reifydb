// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::Token;
use crate::ast::lex::literal::bool::parse_boolean;
use crate::ast::lex::literal::number::parse_number;
use crate::ast::lex::literal::temporal::parse_temporal;
use crate::ast::lex::literal::text::parse_text;
use crate::ast::lex::literal::undefined::parse_undefined;
use nom::branch::alt;
use nom::character::multispace0;
use nom::sequence::preceded;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

mod bool;
mod number;
mod temporal;
mod text;
mod undefined;

/// Parses any literal
pub fn parse_literal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    preceded(
        multispace0(),
        alt((parse_text, parse_number, parse_boolean, parse_undefined)),
    )
    .parse(input)
}
