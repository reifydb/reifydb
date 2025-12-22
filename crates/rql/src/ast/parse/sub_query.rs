// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstStatement, AstSubQuery,
	parse::{Parser, Precedence},
	tokenize::Operator::{self, CloseCurly, OpenCurly},
};

impl Parser {
	/// Parse a subquery enclosed in braces: { query statement }
	pub(crate) fn parse_sub_query(&mut self) -> crate::Result<AstSubQuery> {
		let token = self.consume_operator(OpenCurly)?;
		let statement = self.parse_sub_query_statement()?;
		self.consume_operator(CloseCurly)?;

		Ok(AstSubQuery {
			token,
			statement,
		})
	}

	/// Parse the statement inside a subquery
	/// This is similar to the main parse() but stops at CloseCurly
	fn parse_sub_query_statement(&mut self) -> crate::Result<AstStatement> {
		let mut nodes = Vec::new();
		let mut has_pipes = false;

		loop {
			// Stop if we hit the closing brace or EOF
			if self.is_eof() || self.current()?.is_operator(CloseCurly) {
				break;
			}

			// Parse the next node
			nodes.push(self.parse_node(Precedence::None)?);

			// Check for pipe operator as separator between nodes
			if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
				self.advance()?; // consume the pipe
				has_pipes = true;
			} else if !self.is_eof() && !self.current()?.is_operator(CloseCurly) {
				// If there's no pipe and we're not at the end, this might be an error
				// For now, we'll just continue parsing
				break;
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
		})
	}
}
