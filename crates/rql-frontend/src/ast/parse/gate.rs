// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::ValueError;

use crate::{
	ast::{ast::AstGate, parse::Parser},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_gate(&mut self) -> Result<AstGate<'bump>, ValueError> {
		let (token, node, rql) = self.parse_keyword_with_optional_braces_single(Keyword::Gate)?;
		Ok(AstGate {
			token,
			node,
			rql,
		})
	}
}
