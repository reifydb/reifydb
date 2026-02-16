// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::diagnostic::ast, return_error};

use crate::{
	ast::{
		ast::{AstPolicy, AstPolicyBlock, AstPolicyKind},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_policy_block(&mut self) -> crate::Result<AstPolicyBlock<'bump>> {
		let token = self.consume_keyword(Keyword::Policy)?;
		self.consume_operator(Operator::OpenCurly)?;

		let mut policies = Vec::new();
		loop {
			let (token, policy) = self.parse_policy_kind()?;
			let value = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

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

	fn parse_policy_kind(&mut self) -> crate::Result<(Token<'bump>, AstPolicyKind)> {
		let identifier = self.consume(TokenKind::Identifier)?;
		let ty = match identifier.fragment.text() {
			"saturation" => AstPolicyKind::Saturation,
			"default" => AstPolicyKind::Default,
			"not" => {
				self.consume_literal(Literal::None)?;
				AstPolicyKind::NotNone
			}
			_ => return_error!(ast::invalid_policy_error(identifier.fragment.to_owned())),
		};

		Ok((identifier, ty))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{AstCreate, AstCreateTable, AstPolicyKind, AstType},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_saturation_error() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"policy {saturation error}"#).unwrap().into_iter().collect();

		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"policy {saturation none}"#).unwrap().into_iter().collect();

		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse_policy_block().unwrap();
		assert_eq!(result.policies.len(), 1);

		let policies = result.policies;
		assert_eq!(policies.len(), 1);

		let saturation = &policies[0];
		assert!(matches!(saturation.policy, AstPolicyKind::Saturation));
		assert_eq!(saturation.value.as_literal_none().value(), "none");
	}

	#[test]
	fn test_table_with_policy_block() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
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
		.unwrap()
		.into_iter()
		.collect();

		let mut parser = Parser::new(&bump, tokens);
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
				assert_eq!(table.namespace[0].text(), "test");
				assert_eq!(table.name.text(), "items");
				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstType::Unconstrained(id) => {
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
