// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::row::{JoinTtl, OperatorTtl, Ttl};
use reifydb_value::value::temporal::parse::duration::parse_duration;

use crate::{
	Result,
	ast::ast::{AstJoinTtl, AstTtl},
	diagnostic::AstError,
	plan::logical::Compiler,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_operator_ttl(ast: AstTtl<'bump>) -> Result<OperatorTtl> {
		if let Some(token) = &ast.announce {
			return Err(AstError::UnexpectedToken {
				expected: "no 'announce' clause: operator state is excluded from CDC".to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		Ok(OperatorTtl {
			duration: Self::compile_ttl(ast)?.duration,
		})
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
				expected: "no 'on' clause: a TTL expires on the row's own last write".to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let announce = match &ast.announce {
			None => false,
			Some(token) => match token.fragment.text().to_lowercase().as_str() {
				"true" => true,
				"false" => false,
				_ => {
					return Err(AstError::UnexpectedToken {
						expected: "'true' or 'false'".to_string(),
						fragment: token.fragment.to_owned(),
					}
					.into());
				}
			},
		};

		Ok(Ttl {
			duration,
			announce,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::version_epoch::EpochRetention;

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
			announce: None,
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
			announce: None,
		})
	}

	#[test]
	fn compile_ttl_accepts_a_sub_second_ttl() {
		// The old 1s floor existed because expiry resolved through the version epoch one
		// whole-second bucket at a time, so a shorter ttl could not be honoured to anywhere near
		// its stated precision. Expiry now compares the row's own timestamp against the cutoff
		// instant, so sub-second durations mean exactly what they say and must be declarable.
		assert!(compile("'500ms'").is_ok(), "a sub-second ttl must compile now that expiry is per-row");
		assert!(compile("'1ms'").is_ok(), "and precision goes well below that");
	}

	#[test]
	fn compile_ttl_accepts_a_ttl_beyond_the_old_epoch_coverage() {
		// The old ceiling was the epoch's guaranteed coverage: past it floor_version_at yielded no
		// cutoff, so the class silently reclaimed nothing. Nothing consults the epoch for a row or
		// operator ttl any more, so a long horizon is just a long horizon.
		let beyond = format!(
			"'{}d'",
			EpochRetention::default().guaranteed_coverage().seconds() / (24 * 60 * 60) + 1
		);

		assert!(compile(&beyond).is_ok(), "a ttl past the old epoch coverage must compile: {beyond}");
		assert!(compile("'3650d'").is_ok(), "and there is no upper bound short of duration overflow");
	}

	#[test]
	fn compile_ttl_still_rejects_a_non_positive_ttl() {
		// The one bound that survives, and the only one that was ever about the ttl itself rather
		// than about the mechanism behind it: a zero or negative ttl states that rows expire before
		// they are written, which no cutoff arithmetic can honour.
		assert!(compile("'0s'").is_err(), "a zero ttl must not compile");
	}
}
