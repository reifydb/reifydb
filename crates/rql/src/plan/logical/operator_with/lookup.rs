// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{operator_with::LookupWith, row::OperatorRetention};

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
	pub(crate) fn compile_lookup_with(with: Option<&AstOperatorWith<'bump>>) -> Result<LookupWith> {
		let mut lookup = LookupWith::default();
		for entry in entries(with) {
			match entry.key.word() {
				Some("retention") => lookup.retention = Some(compile_lookup_retention(entry)?),
				_ => return Err(unknown_key(entry, "retention")),
			}
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
	left.ok_or_else(|| unknown_key(entry, "'left'"))
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
		Compiler::compile_lookup_with(lookup.with.as_ref())
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
		// Both forms store read versions, so the left form takes the same left retention as the inner one.
		let with = lookup_with(
			"left lookup { from orders } as o using (id, o.user_id) with { retention: { left: 1h } }",
		)
		.unwrap();
		assert_eq!(with.retention.unwrap().duration, Duration::from_hours(1).unwrap());
	}

	#[test]
	fn missing_with_leaves_retention_unset() {
		// Retention is optional like the join's, so no with must compile and leave the left side unbounded.
		let with = lookup_with(&lookup("")).unwrap();
		assert!(with.retention.is_none());
	}

	#[test]
	fn empty_retention_block_is_ast_005() {
		// An empty block bounds no side, so it must be refused like the join's.
		assert_eq!(code(&lookup(" with { retention: { } }")), "AST_005");
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
