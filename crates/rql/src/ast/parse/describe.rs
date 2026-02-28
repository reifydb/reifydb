// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
			ast::{AstCast, AstDescribe},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn describe_query() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "describe { map {cast(9924, int8)} }").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
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
