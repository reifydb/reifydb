// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{AstCast, parse::Parser, tokenize::Keyword::Cast};

impl Parser {
	pub(crate) fn parse_cast(&mut self) -> crate::Result<AstCast> {
		let token = self.consume_keyword(Cast)?;
		let tuple = self.parse_tuple()?;
		Ok(AstCast {
			token,
			tuple,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{AstCast, parse::parse, tokenize::tokenize};

	#[test]
	fn test_cast() {
		let tokens = tokenize("cast(9924, int8)").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let AstCast {
			tuple,
			..
		} = result[0].first_unchecked().as_cast();
		assert_eq!(tuple.len(), 2);

		assert_eq!(tuple.nodes[0].as_literal_number().value(), "9924");
		assert!(matches!(
			tuple.nodes[1].as_identifier().value(),
			"int8"
		));
	}
}
