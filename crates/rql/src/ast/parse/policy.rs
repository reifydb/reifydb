// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::diagnostic::ast, return_error};

use crate::ast::{
	ast::{AstPolicy, AstPolicyBlock, AstPolicyKind},
	parse::{Parser, Precedence},
	tokenize::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, Token, TokenKind},
	},
};

impl Parser {
	pub(crate) fn parse_policy_block(&mut self) -> crate::Result<AstPolicyBlock> {
		let token = self.consume_keyword(Keyword::Policy)?;
		self.consume_operator(Operator::OpenCurly)?;

		let mut policies = Vec::new();
		loop {
			let (token, policy) = self.parse_policy_kind()?;
			let value = Box::new(self.parse_node(Precedence::None)?);

			policies.push(AstPolicy {
				token,
				policy,
				value,
			});

			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_none() {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;
		Ok(AstPolicyBlock {
			token,
			policies,
		})
	}

	fn parse_policy_kind(&mut self) -> crate::Result<(Token, AstPolicyKind)> {
		let identifier = self.consume(TokenKind::Identifier)?;
		let ty = match identifier.fragment.text() {
			"saturation" => AstPolicyKind::Saturation,
			"default" => AstPolicyKind::Default,
			"not" => {
				self.consume_literal(Literal::Undefined)?;
				AstPolicyKind::NotUndefined
			}
			_ => return_error!(ast::invalid_policy_error(identifier.fragment)),
		};

		Ok((identifier, ty))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::ast::{
		ast::{AstCreate, AstCreateTable, AstDataType, AstPolicyKind},
		parse::Parser,
		tokenize::tokenize,
	};

	#[test]
	fn test_saturation_error() {
		let tokens = tokenize(r#"policy {saturation error}"#).unwrap();

		let mut parser = Parser::new(tokens);
		let result = parser.parse_policy_block().unwrap();
		assert_eq!(result.policies.len(), 1);

		let policies = result.policies;
		assert_eq!(policies.len(), 1);

		let saturation = &policies[0];
		assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
		assert_eq!(saturation.value.as_identifier().text(), "error");
	}

	#[test]
	fn test_saturation_undefined() {
		let tokens = tokenize(r#"policy {saturation undefined}"#).unwrap();

		let mut parser = Parser::new(tokens);
		let result = parser.parse_policy_block().unwrap();
		assert_eq!(result.policies.len(), 1);

		let policies = result.policies;
		assert_eq!(policies.len(), 1);

		let saturation = &policies[0];
		assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
		assert_eq!(saturation.value.as_literal_undefined().value(), "undefined");
	}

	#[test]
	fn test_table_with_policy_block() {
		let tokens = tokenize(
			r#"
        create table test.items{
            field:  int2
                    policy {
                        saturation error,
                        default 0
                    }
        }
    "#,
		)
		.unwrap();

		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Table(AstCreateTable {
				table,
				columns,
				..
			}) => {
				assert_eq!(table.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(table.name.text(), "items");
				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstDataType::Unconstrained(id) => {
						assert_eq!(id.text(), "int2")
					}
					_ => panic!("Expected simple data type"),
				}

				let policies = col.policies.as_ref().unwrap();
				assert_eq!(policies.policies.len(), 2);

				let saturation = &policies.policies[0];
				assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
				assert_eq!(saturation.value.as_identifier().text(), "error");

				let default = &policies.policies[1];
				assert!(matches!(default.policy, AstPolicyKind::Default));
				assert_eq!(default.value.as_literal_number().value(), "0");
			}
			_ => unreachable!(),
		}
	}
}
