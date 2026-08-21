// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::row::{JoinRetention, OperatorRetention, Ttl};

use crate::{
	Result,
	ast::ast::{AstJoinRetention, AstOperatorRetention, AstTtl},
	diagnostic::AstError,
	duration::{DurationBound, compile_duration},
	plan::logical::Compiler,
	token::token::Token,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_operator_retention(ast: AstOperatorRetention<'bump>) -> Result<OperatorRetention> {
		let duration = compile_duration(&ast.duration, DurationBound::Positive, "a retention")?;

		if let Some(token) = &ast.anchor {
			return Err(AstError::UnexpectedToken {
				expected: "no 'on' clause: a retention expires on the row's own last write".to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		Ok(OperatorRetention {
			duration,
		})
	}

	pub(crate) fn compile_join_retention(ast: AstJoinRetention<'bump>) -> Result<JoinRetention> {
		let left = match ast.left {
			Some(side) => Some(Self::compile_side_retention(side)?),
			None => None,
		};
		let right = match ast.right {
			Some(side) => Some(Self::compile_side_retention(side)?),
			None => None,
		};
		Ok(JoinRetention {
			left,
			right,
		})
	}

	fn compile_side_retention(token: Token<'bump>) -> Result<OperatorRetention> {
		Self::compile_operator_retention(AstOperatorRetention {
			duration: token,
			anchor: None,
		})
	}

	pub(crate) fn compile_ttl(ast: AstTtl<'bump>) -> Result<Ttl> {
		let duration = compile_duration(&ast.duration, DurationBound::Positive, "a TTL")?;

		if let Some(token) = &ast.anchor {
			return Err(AstError::UnexpectedToken {
				expected: "no 'on' clause: a TTL expires on the row's own last write".to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		Ok(Ttl {
			duration,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::version_epoch::EpochRetention;

	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn compile_ttl_accepts_compound_duration() {
		// Duration::Display emits the compound form ("2d2h" for 50h), so generated MIGRATE statements carry
		// it back in and it has to compile rather than fault.
		let bump = Bump::new();
		let tokens = tokenize(&bump, "2d2h").unwrap();
		let duration = tokens.into_iter().next().unwrap();
		let ttl = Compiler::<'_>::compile_ttl(AstTtl {
			duration,
			anchor: None,
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
		})
	}

	#[test]
	fn compile_ttl_accepts_a_sub_second_ttl() {
		// The 1s floor existed while expiry resolved through the version epoch a whole second at a time.
		// Expiry now compares each row's own timestamp against the cutoff, so sub-second spans are honoured.
		assert!(compile("500ms").is_ok(), "a sub-second ttl must compile now that expiry is per-row");
		assert!(compile("1ms").is_ok(), "and precision goes well below that");
	}

	#[test]
	fn compile_ttl_accepts_a_ttl_beyond_the_old_epoch_coverage() {
		// The ceiling was the epoch's guaranteed coverage, past which no cutoff resolved and the class
		// silently reclaimed nothing. No row or operator ttl consults the epoch any more.
		let beyond =
			format!("{}d", EpochRetention::default().guaranteed_coverage().seconds() / (24 * 60 * 60) + 1);

		assert!(compile(&beyond).is_ok(), "a ttl past the old epoch coverage must compile: {beyond}");
		assert!(compile("3650d").is_ok(), "and there is no upper bound short of duration overflow");
	}

	#[test]
	fn compile_ttl_still_rejects_a_non_positive_ttl() {
		// The only bound about the ttl itself rather than the mechanism behind it: a non-positive ttl says
		// rows expire before they are written, which no cutoff arithmetic can honour.
		assert!(compile("0s").is_err(), "a zero ttl must not compile");
	}
}
