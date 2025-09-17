// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstDescribe,
	parse::{Parser, Precedence},
	tokenize::{Keyword::Describe, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_describe(&mut self) -> crate::Result<AstDescribe<'a>> {
		let token = self.consume_keyword(Describe)?;
		self.consume_operator(Operator::OpenCurly)?;
		let node = Box::new(self.parse_node(Precedence::None)?);
		self.consume_operator(Operator::CloseCurly)?;
		Ok(AstDescribe::Query {
			token,
			node,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstCast, AstDescribe, parse::parse, tokenize::tokenize};

	#[test]
	fn describe_query() {
		let tokens = tokenize("describe { map cast(9924, int8) }").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		match result.first().unwrap().first_unchecked().as_describe() {
			AstDescribe::Query {
				node,
				..
			} => {
				let map = node.as_map();
				assert_eq!(map.nodes.len(), 1);

				let AstCast {
					tuple,
					..
				} = map.nodes[0].as_cast();
				assert_eq!(tuple.len(), 2);

				assert_eq!(tuple.nodes[0].as_literal_number().value(), "9924");
				assert!(matches!(tuple.nodes[1].as_identifier().text(), "int8"));
			}
		};
	}
}
