// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstCast, parse::Parser},
	token::keyword::Keyword::Cast,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_cast(&mut self) -> crate::Result<AstCast<'bump>> {
		let token = self.consume_keyword(Cast)?;
		let tuple = self.parse_tuple()?;
		Ok(AstCast {
			token,
			tuple,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{ast::AstCast, parse::parse},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_cast() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "cast(9924, int8)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let AstCast {
			tuple,
			..
		} = result[0].first_unchecked().as_cast();
		assert_eq!(tuple.len(), 2);

		assert_eq!(tuple.nodes[0].as_literal_number().value(), "9924");
		assert!(matches!(tuple.nodes[1].as_identifier().text(), "int8"));
	}
}
