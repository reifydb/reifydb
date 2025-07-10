// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Keyword::Describe;
use crate::ast::lex::Operator;
use crate::ast::parse::{Parser, Precedence};
use crate::ast::{parse, AstDescribe};

impl Parser {
	pub(crate) fn parse_describe(&mut self) -> parse::Result<AstDescribe> {
		let token = self.consume_keyword(Describe)?;
		self.consume_operator(Operator::OpenParen)?;
		let node = Box::new(self.parse_node(Precedence::None)?);
		self.consume_operator(Operator::CloseParen)?;
		Ok(AstDescribe::Query { token, node })
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::lex::lex;
	use crate::ast::parse::parse;
	use crate::ast::{AstCast, AstDescribe, AstDataType};

	#[test]
	fn describe_query() {
		let tokens = lex("describe ( select cast(9924, int8) )").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		match result.first().unwrap().first_unchecked().as_describe() {
			AstDescribe::Query { node, .. } => {
				let select = node.as_select();
				assert_eq!(select.select.len(), 1);

				let AstCast { tuple, .. } = select.select[0].as_cast();
				assert_eq!(tuple.len(), 2);

				assert_eq!(tuple.nodes[0].as_literal_number().value(), "9924");
				assert!(matches!(tuple.nodes[1].as_kind(), AstDataType::Int8(_)));
			}
		};
	}
}
