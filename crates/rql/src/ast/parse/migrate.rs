// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{AstMigrate, AstRollbackMigration},
		parse::Parser,
	},
	token::{
		keyword::Keyword,
		token::{Literal, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	/// Parses `MIGRATE` or `MIGRATE TO 'migration_name'`
	pub(crate) fn parse_migrate(&mut self) -> Result<AstMigrate<'bump>> {
		let token = self.consume_keyword(Keyword::Migrate)?;

		let target = if (self.consume_if(TokenKind::Keyword(Keyword::To))?).is_some() {
			match &self.current()?.kind {
				TokenKind::Literal(Literal::Text) => {
					let text = self.current()?.fragment.text().to_string();
					self.advance()?;
					Some(text)
				}
				_ => {
					let fragment = self.current()?.fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::UnexpectedToken {
							expected: "migration name as string literal".to_string(),
						},
						message: format!(
							"Expected migration name as string literal after TO, got {}",
							fragment.text()
						),
						fragment,
					}));
				}
			}
		} else {
			None
		};

		Ok(AstMigrate {
			token,
			target,
		})
	}

	/// Parses `ROLLBACK MIGRATION` or `ROLLBACK MIGRATION TO 'migration_name'`
	pub(crate) fn parse_rollback_migration(&mut self) -> Result<AstRollbackMigration<'bump>> {
		let token = self.consume_keyword(Keyword::Rollback)?;
		self.consume_keyword(Keyword::Migration)?;

		let target = if (self.consume_if(TokenKind::Keyword(Keyword::To))?).is_some() {
			match &self.current()?.kind {
				TokenKind::Literal(Literal::Text) => {
					let text = self.current()?.fragment.text().to_string();
					self.advance()?;
					Some(text)
				}
				_ => {
					let fragment = self.current()?.fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::UnexpectedToken {
							expected: "migration name as string literal".to_string(),
						},
						message: format!(
							"Expected migration name as string literal after TO, got {}",
							fragment.text()
						),
						fragment,
					}));
				}
			}
		} else {
			None
		};

		Ok(AstRollbackMigration {
			token,
			target,
		})
	}
}
