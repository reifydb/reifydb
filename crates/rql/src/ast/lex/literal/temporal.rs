// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::Token;
use nom::IResult;
use nom_locate::LocatedSpan;

pub(crate) fn parse_temporal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    todo!()
}
