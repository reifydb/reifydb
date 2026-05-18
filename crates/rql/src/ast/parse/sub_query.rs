// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstStatement, AstSubQuery},
		parse::{Parser, Precedence},
	},
	token::operator::Operator::{self, CloseCurly, OpenCurly},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_sub_query(&mut self) -> Result<AstSubQuery<'bump>> {
		let token = self.consume_operator(OpenCurly)?;
		let statement = self.parse_sub_query_statement()?;
		self.consume_operator(CloseCurly)?;

		Ok(AstSubQuery {
			token,
			statement,
		})
	}

	fn parse_sub_query_statement(&mut self) -> Result<AstStatement<'bump>> {
		let mut nodes = Vec::new();
		let mut has_pipes = false;

		loop {
			if self.is_eof() || self.current()?.is_operator(CloseCurly) {
				break;
			}

			nodes.push(self.parse_node(Precedence::None)?);

			if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
				self.advance()?;
				has_pipes = true;
			} else if !self.is_eof() && !self.current()?.is_operator(CloseCurly) {
				break;
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
			is_output: false,
			rql: "",
		})
	}
}
