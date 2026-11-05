// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{ast::AstChanges, parse::Parser},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_changes(&mut self) -> Result<AstChanges<'bump>> {
		let token = self.consume_keyword(Keyword::Changes)?;
		let with = self.parse_operator_with()?;
		Ok(AstChanges {
			token,
			with,
		})
	}
}
