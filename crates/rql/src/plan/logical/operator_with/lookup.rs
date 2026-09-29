// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::operation::lookup_retention_left_missing, operator_with::LookupWith, row::OperatorRetention,
};
use reifydb_value::{error, fragment::Fragment};

use crate::{
	Result,
	ast::ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithValue},
	duration::{DurationBound, compile_duration},
	plan::logical::{
		Compiler,
		operator_with::{entries, literal, unknown_key},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_lookup_with(
		with: Option<&AstOperatorWith<'bump>>,
		fragment: Fragment,
	) -> Result<LookupWith> {
		let mut lookup = LookupWith::default();
		for entry in entries(with) {
			match entry.key.word() {
				Some("retention") => lookup.retention = Some(compile_lookup_retention(entry)?),
				_ => return Err(unknown_key(entry, "retention")),
			}
		}
		if lookup.retention.is_none() {
			return Err(error!(lookup_retention_left_missing(fragment)));
		}
		Ok(lookup)
	}
}

fn compile_lookup_retention(entry: &AstOperatorWithEntry<'_>) -> Result<OperatorRetention> {
	let Some(AstOperatorWithValue::Block(sides)) = &entry.value else {
		return Err(unknown_key(entry, "a block such as { left: 10s }"));
	};
	let mut left = None;
	for side in sides {
		match side.key.word() {
			Some("left") => {
				left = Some(OperatorRetention {
					duration: compile_duration(
						literal(side)?,
						DurationBound::Positive,
						"the 'left' side",
					)?,
				});
			}
			_ => return Err(unknown_key(side, "'left'")),
		}
	}
	left.ok_or_else(|| error!(lookup_retention_left_missing(entry.key.fragment())))
}

#[cfg(test)]
mod tests {
	use reifydb_core::operator_with::LookupWith;
	use reifydb_value::value::duration::Duration;

	use crate::{
		Result,
		ast::{ast::Ast, parse_str},
		bump::Bump,
		plan::logical::Compiler,
	};

	fn lookup_with(source: &str) -> Result<LookupWith> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source)?;
		let Some(Ast::Lookup(lookup)) = statements[0].nodes.first() else {
			panic!("expected a lookup node in: {source}");
		};
		Compiler::compile_lookup_with(lookup.with.as_ref(), lookup.token.fragment.to_owned())
	}

	fn lookup(with: &str) -> String {
		format!("inner lookup {{ from orders }} as o using (id, o.user_id){with}")
	}

	fn code(source: &str) -> String {
		lookup_with(source).expect_err(&format!("must be rejected: {source}")).diagnostic().code
	}

	#[test]
	fn retention_left_is_kept() {
		// The left retention bounds the stored read versions and the lease, so it must survive as written.
		let with = lookup_with(&lookup(" with { retention: { left: 10s } }")).unwrap();
		let retention = with.retention.expect("retention must be set");
		assert_eq!(retention.duration, Duration::from_seconds(10).unwrap());
	}

	#[test]
	fn left_lookup_takes_the_same_retention() {
		// Both forms store read versions, so the left form needs the same left retention as the inner one.
		let with = lookup_with(
			"left lookup { from orders } as o using (id, o.user_id) with { retention: { left: 1h } }",
		)
		.unwrap();
		assert_eq!(with.retention.unwrap().duration, Duration::from_hours(1).unwrap());
	}

	#[test]
	fn missing_with_is_lookup_006() {
		// Without a left retention the lease never moves and GC keeps every version, so no with is refused.
		assert_eq!(code(&lookup("")), "LOOKUP_006");
	}

	#[test]
	fn missing_with_points_at_the_lookup() {
		// Nothing is there to point at, so the error must name the lookup keyword the author has to extend.
		let err = lookup_with(&lookup("")).unwrap_err();
		assert_eq!(err.fragment.text(), "inner");
	}

	#[test]
	fn empty_retention_block_is_lookup_006() {
		// An empty block still has no left side, so it must fail the same way as a missing with.
		assert_eq!(code(&lookup(" with { retention: { } }")), "LOOKUP_006");
	}

	#[test]
	fn retention_right_is_rejected() {
		// A lookup keeps no right rows, so a right retention has nothing to expire and must be refused.
		assert_eq!(code(&lookup(" with { retention: { left: 10s, right: 10s } }")), "AST_005");
		assert_eq!(code(&lookup(" with { retention: { right: 10s } }")), "AST_005");
	}

	#[test]
	fn join_only_keys_are_rejected() {
		// snapshot, latest and earliest have no meaning for a lookup; only retention is accepted.
		for key in ["snapshot: true", "latest: true", "earliest: true"] {
			let source = lookup(&format!(" with {{ retention: {{ left: 10s }}, {key} }}"));
			assert_eq!(code(&source), "AST_005", "wrong diagnostic for: {source}");
		}
	}

	#[test]
	fn a_rejected_key_points_at_that_key() {
		// The span must land on the key the author has to remove, not on the lookup.
		let err = lookup_with(&lookup(" with { retention: { left: 10s }, snapshot: true }")).unwrap_err();
		assert_eq!(err.fragment.text(), "snapshot");
	}

	#[test]
	fn retention_shorthand_is_rejected() {
		// The join's per-side block shape is required; a bare duration does not say which side it bounds.
		assert_eq!(code(&lookup(" with { retention: 10s }")), "AST_005");
	}

	#[test]
	fn non_positive_retention_is_rejected() {
		// A zero retention would free every left row at once and leave no read version to undo against.
		assert!(lookup_with(&lookup(" with { retention: { left: 0s } }")).is_err());
	}
}
