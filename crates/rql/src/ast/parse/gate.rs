// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
