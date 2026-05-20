// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{ast::AstGate, parse::Parser},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_gate(&mut self) -> Result<AstGate<'bump>> {
		let (token, node, rql) = self.parse_keyword_with_optional_braces_single(Keyword::Gate)?;
		Ok(AstGate {
			token,
			node,
			rql,
		})
	}
}
