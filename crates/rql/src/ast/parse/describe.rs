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
	use bumpalo::Bump;

	use crate::{
		ast::{
			ast::{Ast, AstCast, AstDescribe, AstLiteral, AstType},
			parse::parse,
		},
		token::tokenize,
	};

	#[test]
	fn describe_query() {
		let bump = Bump::new();
		let source = "describe { map {cast(9924, int8)} }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Some(Ast::Describe(describe)) = result.first().unwrap().nodes.first() else {
			panic!("not describe")
		};
		match describe {
			AstDescribe::Query {
				node,
				..
			} => {
				let Ast::Map(map) = &**node else {
					panic!("not map")
				};
				assert_eq!(map.nodes.len(), 1);

				let Ast::Cast(AstCast {
					expression,
					ty,
					..
				}) = &map.nodes[0]
				else {
					panic!("not cast")
				};
				let Ast::Literal(AstLiteral::Number(expression_number)) = &**expression else {
					panic!("not number")
				};
				assert_eq!(expression_number.value(), "9924");
				assert!(matches!(ty, AstType::Unconstrained(name) if name.text() == "int8"));
			}
		};
	}
}
