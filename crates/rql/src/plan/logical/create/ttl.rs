// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::row::{Ttl, TtlAnchor, TtlCleanupMode};

use crate::{Result, ast::ast::AstTtl, diagnostic::AstError, plan::logical::Compiler};

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

	pub(crate) fn compile_ttl(ast: AstTtl<'bump>) -> Result<Ttl> {
		let duration = Self::parse_duration(ast.duration.fragment.text())?;
		let duration_nanos: u64 = duration.as_nanos().try_into().map_err(|_| AstError::UnexpectedToken {
			expected: "a duration that fits in u64 nanoseconds".to_string(),
			fragment: ast.duration.fragment.to_owned(),
		})?;
		if duration_nanos == 0 {
			return Err(AstError::UnexpectedToken {
				expected: "a non-zero TTL duration".to_string(),
				fragment: ast.duration.fragment.to_owned(),
			}
			.into());
		}

		let anchor = match ast.anchor {
			None => TtlAnchor::Created,
			Some(token) => match token.fragment.text().to_lowercase().as_str() {
				"created" => TtlAnchor::Created,
				"updated" => TtlAnchor::Updated,
				_ => {
					return Err(AstError::UnexpectedToken {
						expected: "'created' or 'updated'".to_string(),
						fragment: token.fragment.to_owned(),
					}
					.into());
				}
			},
		};

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
			duration_nanos,
			anchor,
			cleanup_mode,
		})
	}
}
