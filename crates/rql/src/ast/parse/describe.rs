// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstDescribe,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_describe(&mut self) -> Result<AstDescribe<'bump>> {
		let token = self.consume_keyword(Keyword::Describe)?;
		self.consume_operator(Operator::OpenCurly)?;
		let node = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
		self.consume_operator(Operator::CloseCurly)?;
		Ok(AstDescribe::Query {
			token,
			node,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{AstCast, AstDescribe, AstType},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn describe_query() {
		let bump = Bump::new();
		let source = "describe { map {cast(9924, int8)} }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		match result.first().unwrap().first_unchecked().as_describe() {
			AstDescribe::Query {
				node,
				..
			} => {
				let map = node.as_map();
				assert_eq!(map.nodes.len(), 1);

				let AstCast {
					expression,
					to,
					..
				} = map.nodes[0].as_cast();
				assert_eq!(expression.as_literal_number().value(), "9924");
				assert!(matches!(to, AstType::Unconstrained(name) if name.text() == "int8"));
			}
		};
	}
}
