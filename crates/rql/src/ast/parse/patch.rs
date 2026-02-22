// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstPatch, parse::Parser},
	error::{OperationKind, RqlError},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_patch(&mut self) -> crate::Result<AstPatch<'bump>> {
		let token = self.consume_keyword(Keyword::Patch)?;

		let (nodes, has_braces) = self.parse_expressions(true, false)?;

		if !has_braces {
			return Err(RqlError::OperatorMissingBraces {
				kind: OperationKind::Patch,
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		Ok(AstPatch {
			token,
			assignments: nodes,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{ast::ast::InfixOperator, bump::Bump, token::tokenize};

	#[test]
	fn test_patch_colon_syntax() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "PATCH {status: \"active\"}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let patch = result.first_unchecked().as_patch();
		assert_eq!(patch.assignments.len(), 1);

		let infix = patch.assignments[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));
		assert_eq!(infix.right.as_identifier().text(), "status");
	}

	#[test]
	fn test_patch_multiple_assignments() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "PATCH {status: \"active\", score: 100}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let patch = result.first_unchecked().as_patch();
		assert_eq!(patch.assignments.len(), 2);

		let first_infix = patch.assignments[0].as_infix();
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.right.as_identifier().text(), "status");

		let second_infix = patch.assignments[1].as_infix();
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.right.as_identifier().text(), "score");
	}

	#[test]
	fn test_patch_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "PATCH 1").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);

		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "PATCH_001");
	}
}
