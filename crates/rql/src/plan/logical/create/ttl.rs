// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::row::{JoinTtl, Ttl, TtlCleanupMode};
use reifydb_runtime::version_epoch::{BUCKET_WIDTH_NANOS, EpochRetention, MIN_TTL};
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

		if duration < MIN_TTL {
			return Err(AstError::UnexpectedToken {
				expected: format!(
					"a TTL of at least {MIN_TTL}: expiry resolves through the version epoch at \
					 {}ms granularity, so a shorter TTL cannot be enforced accurately",
					BUCKET_WIDTH_NANOS / 1_000_000
				),
				fragment: ast.duration.fragment.to_owned(),
			}
			.into());
		}

		let coverage = EpochRetention::default().guaranteed_coverage_nanos();
		if duration.to_std().as_nanos() > u128::from(coverage) {
			return Err(AstError::UnexpectedToken {
				expected: format!(
					"a TTL of at most {} days: beyond that the version epoch cannot resolve a \
					 cutoff and the TTL would never expire anything",
					coverage / (24 * 60 * 60 * 1_000_000_000)
				),
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

	fn compile(literal: &str) -> Result<Ttl> {
		let bump = Bump::new();
		let tokens = tokenize(&bump, literal).unwrap();
		let duration = tokens.into_iter().next().unwrap();
		Compiler::<'_>::compile_ttl(AstTtl {
			duration,
			anchor: None,
			mode: None,
		})
	}

	#[test]
	fn compile_ttl_accepts_a_ttl_at_the_declared_minimum() {
		// The minimum itself must remain declarable, or the bound would be off by one and the
		// shortest usable TTL would silently become larger than advertised.
		assert!(compile("'1s'").is_ok(), "a one second ttl is exactly the minimum and must compile");
	}

	#[test]
	fn compile_ttl_rejects_a_ttl_below_the_epoch_resolution() {
		// Expiry resolves through the epoch a bucket at a time, so a sub-second TTL cannot be
		// honoured to anywhere near its stated precision: it would expire late by a large
		// fraction of itself. Rejecting at declaration turns that inaccuracy into an error the
		// author can see rather than a TTL that quietly means something else.
		let error = compile("'500ms'").expect_err("a sub-minimum ttl must not compile");

		assert!(
			format!("{error:?}").contains("at least"),
			"the rejection must state the minimum so the author knows what to raise it to: {error:?}"
		);
	}

	#[test]
	fn compile_ttl_accepts_a_ttl_inside_the_epoch_coverage() {
		// The default horizon floor promises seven days, so a ttl at that scale must remain declarable.
		assert!(compile("'7d'").is_ok(), "a seven day ttl is within epoch coverage and must compile");
	}

	#[test]
	fn compile_ttl_rejects_a_ttl_the_epoch_can_never_resolve() {
		// Past epoch coverage, `floor_version_at` yields no cutoff, so the class reclaims nothing and reports
		// success. Rejecting at declaration turns that silent no-op into an error the author can see.
		let coverage_days =
			EpochRetention::default().guaranteed_coverage_nanos() / (24 * 60 * 60 * 1_000_000_000);
		let beyond = format!("'{}d'", coverage_days + 1);

		let error = compile(&beyond).expect_err("a ttl beyond epoch coverage must not compile");

		assert!(
			format!("{error:?}").contains("version epoch"),
			"the rejection must name the epoch limit so the author knows which bound was hit: {error:?}"
		);
	}
}
