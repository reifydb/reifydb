// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{
	IResult, Parser, branch::alt, character::multispace0,
	sequence::preceded,
};
use nom_locate::LocatedSpan;

use crate::ast::{
	Token,
	lex::literal::{
		bool::parse_boolean, number::parse_number,
		temporal::parse_temporal, text::parse_text,
		undefined::parse_undefined,
	},
};

mod bool;
mod number;
mod temporal;
mod text;
mod undefined;

/// Parses any literal
pub fn parse_literal(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	preceded(
		multispace0(),
		alt((
			parse_text,
			parse_number,
			parse_boolean,
			parse_undefined,
			parse_temporal,
		)),
	)
	.parse(input)
}
