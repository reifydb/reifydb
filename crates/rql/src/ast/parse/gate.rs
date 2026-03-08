// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstGate},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_gate(&mut self) -> Result<AstGate<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Keyword::Gate)?;

		// Check if braces are used (optional)
		let has_braces = !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		let node = if has_braces && self.current()?.is_operator(Operator::CloseCurly) {
			// Empty braces: gate {}
			Ast::Nop
		} else {
			self.parse_node(Precedence::None)?
		};

		if has_braces {
			self.consume_operator(Operator::CloseCurly)?;
		}

		Ok(AstGate {
			token,
			node: BumpBox::new_in(node, self.bump()),
			rql: self.source_since(start),
		})
	}
}
