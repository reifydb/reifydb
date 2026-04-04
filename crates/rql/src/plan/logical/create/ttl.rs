// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::row::{RowTtl, RowTtlAnchor, RowTtlCleanupMode};

use crate::{Result, ast::ast::AstRowTtl, diagnostic::AstError, plan::logical::Compiler};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_row_ttl(ast: AstRowTtl<'bump>) -> Result<RowTtl> {
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
			None => RowTtlAnchor::Created,
			Some(token) => match token.fragment.text().to_lowercase().as_str() {
				"created" => RowTtlAnchor::Created,
				"updated" => RowTtlAnchor::Updated,
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
			None => RowTtlCleanupMode::Drop,
			Some(token) => match token.fragment.text().to_lowercase().as_str() {
				"drop" => RowTtlCleanupMode::Drop,
				"delete" => RowTtlCleanupMode::Delete,
				_ => {
					return Err(AstError::UnexpectedToken {
						expected: "'delete' or 'drop'".to_string(),
						fragment: token.fragment.to_owned(),
					}
					.into());
				}
			},
		};

		Ok(RowTtl {
			duration_nanos,
			anchor,
			cleanup_mode,
		})
	}
}
