// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::row::{JoinTtl, Ttl, TtlCleanupMode};
use reifydb_value::value::temporal::parse::duration::parse_duration;

use crate::{
	Result,
	ast::ast::{AstJoinTtl, AstTtl},
	diagnostic::AstError,
	plan::logical::Compiler,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_operator_ttl(ast: AstTtl<'bump>) -> Result<Ttl> {
		if let Some(token) = &ast.mode
			&& token.fragment.text().to_lowercase() == "delete"
		{
			return Err(AstError::UnexpectedToken {
				expected: "'drop' (operator TTL is silent; 'delete' is not supported)".to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		Self::compile_ttl(ast)
	}

	pub(crate) fn compile_join_ttl(ast: AstJoinTtl<'bump>) -> Result<JoinTtl> {
		let left = match ast.left {
			Some(side) => Some(Self::compile_operator_ttl(side)?),
			None => None,
		};
		let right = match ast.right {
			Some(side) => Some(Self::compile_operator_ttl(side)?),
			None => None,
		};
		Ok(JoinTtl {
			left,
			right,
		})
	}

	pub(crate) fn compile_ttl(ast: AstTtl<'bump>) -> Result<Ttl> {
		let duration = parse_duration(ast.duration.fragment.to_owned())?;
		if !duration.is_positive() {
			return Err(AstError::UnexpectedToken {
				expected: "a positive TTL duration".to_string(),
				fragment: ast.duration.fragment.to_owned(),
			}
			.into());
		}

		if let Some(token) = &ast.anchor {
			return Err(AstError::UnexpectedToken {
				expected: "no 'on' clause: TTL is version-anchored and expires on the last write"
					.to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let cleanup_mode = match ast.mode {
			None => TtlCleanupMode::Drop,
			Some(token) => match token.fragment.text().to_lowercase().as_str() {
				"drop" => TtlCleanupMode::Drop,
				"delete" => TtlCleanupMode::Delete,
				_ => {
					return Err(AstError::UnexpectedToken {
						expected: "'delete' or 'drop'".to_string(),
						fragment: token.fragment.to_owned(),
					}
					.into());
				}
			},
		};

		Ok(Ttl {
			duration,
			cleanup_mode,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	// Intent: a compound TTL duration - the form Duration::Display emits (e.g. "2d2h" for 50h)
	// and that generated MIGRATE statements carry - must compile, not panic with ERR-mod:312.
	// Guards the view-migration boot path that regressed in raptor.
	fn compile_ttl_accepts_compound_duration() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "'2d2h'").unwrap();
		let duration = tokens.into_iter().next().unwrap();
		let ttl = Compiler::<'_>::compile_ttl(AstTtl {
			duration,
			anchor: None,
			mode: None,
		})
		.unwrap();
		assert_eq!(ttl.duration.as_nanos().unwrap(), 50i64 * 3600 * 1_000_000_000);
	}
}
