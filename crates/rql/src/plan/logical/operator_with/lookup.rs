// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::operator_with::{Emit, LookupWith};
use reifydb_rql_frontend::ast::ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithValue};
use reifydb_value::{error::ValueError, value::duration::Duration};

use crate::{
	duration::{DurationBound, compile_duration},
	plan::logical::{
		Compiler,
		operator_with::{boolean, entries, exact_or_default, literal, literal_boolean, unknown_key},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_lookup_with(with: Option<&AstOperatorWith<'bump>>) -> Result<LookupWith, ValueError> {
		let mut lookup = LookupWith::default();
		let mut exact = None;
		let mut forget_key = None;
		for entry in entries(with) {
			match entry.key.word() {
				Some("drop") => lookup.drop = Some(compile_lookup_left_span(entry)?),
				Some("forget") => {
					lookup.forget = Some(compile_lookup_left_span(entry)?);
					forget_key = Some(entry.key.fragment());
				}
				Some("emit") => lookup.emit = compile_lookup_emit(entry)?,
				Some("exact") => exact = Some((boolean(entry)?, entry)),
				_ => return Err(unknown_key(entry, "drop, forget, emit, or exact")),
			}
		}
		lookup.exact = exact_or_default(exact, lookup.emit)?;
		if let Err(mut err) = lookup.check_forget() {
			if let Some(fragment) = forget_key {
				err.with_fragment(fragment);
			}
			return Err(err);
		}
		Ok(lookup)
	}
}

fn compile_lookup_emit(entry: &AstOperatorWithEntry<'_>) -> Result<Emit, ValueError> {
	let token = literal(entry)?;
	match literal_boolean(token) {
		Some(on) => Ok(Emit::from(on)),
		None => Ok(Emit::After(compile_duration(token, DurationBound::Positive, "the 'emit' option")?)),
	}
}

fn compile_lookup_left_span(entry: &AstOperatorWithEntry<'_>) -> Result<Duration, ValueError> {
	let Some(AstOperatorWithValue::Block(sides)) = &entry.value else {
		return Err(unknown_key(entry, "a block such as { left: 10s }"));
	};
	let mut left = None;
	for side in sides {
		match side.key.word() {
			Some("left") => {
				left = Some(compile_duration(
					literal(side)?,
					DurationBound::Positive,
					"the 'left' side",
				)?);
			}
			_ => return Err(unknown_key(side, "'left'")),
		}
	}
	left.ok_or_else(|| unknown_key(entry, "'left'"))
}

#[cfg(test)]
mod tests {
	use reifydb_core::operator_with::{Emit, LookupWith};
	use reifydb_rql_frontend::{
		ast::{ast::Ast, parse_str},
		bump::Bump,
	};
	use reifydb_value::{error::ValueError, value::duration::Duration};

	use crate::plan::logical::Compiler;

	fn lookup_with(source: &str) -> Result<LookupWith, ValueError> {
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
	fn forget_left_is_kept() {
		// The left forget bounds the stored read versions and the lease, so it must survive as written.
		let with = lookup_with(&lookup(" with { emit: false, forget: { left: 10s } }")).unwrap();
		let forget = with.forget.expect("forget must be set");
		assert_eq!(forget, Duration::from_seconds(10).unwrap());
	}

	#[test]
	fn left_lookup_takes_the_same_forget() {
		// Both forms store read versions, so the left form takes the same left forget as the inner one.
		let with = lookup_with(
			"left lookup { from orders } as o using (id, o.user_id) with { emit: false, forget: { left: 1h } }",
		)
		.unwrap();
		assert_eq!(with.forget.unwrap(), Duration::from_hours(1).unwrap());
	}

	#[test]
	fn drop_and_forget_parse_into_their_own_fields() {
		// A drop stored as a forget would free left rows the author only meant to stop accepting.
		let with = lookup_with(&lookup(" with { drop: { left: 10s }, forget: { left: 1h } }")).unwrap();
		assert_eq!(with.drop, Some(Duration::from_seconds(10).unwrap()));
		assert_eq!(with.forget, Some(Duration::from_hours(1).unwrap()));

		let drop_only = lookup_with(&lookup(" with { drop: { left: 10s } }")).unwrap();
		assert_eq!(drop_only.drop, Some(Duration::from_seconds(10).unwrap()));
		assert!(drop_only.forget.is_none(), "a drop alone must never free stored rows");
		assert_eq!(code(&lookup(" with { drop: { right: 10s } }")), "AST_005");
	}

	#[test]
	fn a_forget_below_the_drop_is_refused_at_the_forget_key() {
		// Stored rows must outlive the arrivals still accepted for them, and the span must name the key to fix.
		for with in [
			" with { drop: { left: 1h }, forget: { left: 10s } }",
			" with { forget: { left: 10s }, drop: { left: 1h } }",
		] {
			let err = lookup_with(&lookup(with)).expect_err(&format!("must be refused: {with}"));
			assert_eq!(err.code, "FLOW_081", "wrong diagnostic for: {with}");
			assert_eq!(err.fragment.text(), "forget", "wrong span for: {with}");
		}
		assert!(lookup_with(&lookup(" with { drop: { left: 1h }, forget: { left: 1h } }")).is_ok());
	}

	#[test]
	fn missing_with_leaves_forget_unset() {
		// Forget is optional like the join's, so no with must compile and leave the left side unbounded.
		let with = lookup_with(&lookup("")).unwrap();
		assert!(with.forget.is_none());
	}

	#[test]
	fn empty_forget_block_is_ast_005() {
		// An empty block bounds no side, so it must be refused like the join's.
		assert_eq!(code(&lookup(" with { forget: { } }")), "AST_005");
	}

	#[test]
	fn forget_right_is_rejected() {
		// A lookup keeps no right rows, so a right forget has nothing to forget and must be refused.
		assert_eq!(code(&lookup(" with { forget: { left: 10s, right: 10s } }")), "AST_005");
		assert_eq!(code(&lookup(" with { forget: { right: 10s } }")), "AST_005");
	}

	#[test]
	fn join_only_keys_are_rejected() {
		// snapshot, latest and earliest have no meaning for a lookup; only forget is accepted.
		for key in ["snapshot: true", "latest: true", "earliest: true"] {
			let source = lookup(&format!(" with {{ forget: {{ left: 10s }}, {key} }}"));
			assert_eq!(code(&source), "AST_005", "wrong diagnostic for: {source}");
		}
	}

	#[test]
	fn a_rejected_key_points_at_that_key() {
		// The span must land on the key the author has to remove, not on the lookup.
		let err = lookup_with(&lookup(" with { forget: { left: 10s }, snapshot: true }")).unwrap_err();
		assert_eq!(err.fragment.text(), "snapshot");
	}

	#[test]
	fn forget_shorthand_is_rejected() {
		// The join's per-side block shape is required; a bare duration does not say which side it bounds.
		assert_eq!(code(&lookup(" with { forget: 10s }")), "AST_005");
	}

	#[test]
	fn non_positive_forget_is_rejected() {
		// A zero forget would free every left row at once and leave no read version to undo against.
		assert!(lookup_with(&lookup(" with { forget: { left: 0s } }")).is_err());
	}

	#[test]
	fn emit_takes_a_boolean_or_a_positive_duration() {
		// a plain lookup re-emits, so turning it off or delaying it must both be spelled and kept.
		assert_eq!(lookup_with(&lookup(" with { emit: true }")).unwrap().emit, Emit::On);
		assert_eq!(lookup_with(&lookup(" with { emit: false }")).unwrap().emit, Emit::Off);
		assert_eq!(
			lookup_with(&lookup(" with { emit: 1s }")).unwrap().emit,
			Emit::After(Duration::from_seconds(1).unwrap())
		);
		assert_eq!(lookup_with(&lookup("")).unwrap().emit, Emit::On, "re-emit is the default");
	}

	#[test]
	fn emit_refuses_a_zero_duration() {
		// a zero delay is emit: true through extra machinery, so it must be written as what it is.
		assert!(lookup_with(&lookup(" with { emit: 0s }")).is_err());
	}

	#[test]
	fn an_emit_off_lookup_is_exact_unless_it_opts_out() {
		// Exact is the safe default, so dropping to step-end reads must be written out as exact: false.
		assert!(lookup_with(&lookup(" with { emit: false }")).unwrap().exact);
		assert!(!lookup_with(&lookup(" with { emit: false, exact: false }")).unwrap().exact);
		assert!(!lookup_with(&lookup("")).unwrap().exact, "a re-emitting lookup is never exact");
		assert!(!lookup_with(&lookup(" with { emit: 1s }")).unwrap().exact, "a delayed lookup is never exact");
	}

	#[test]
	fn exact_true_with_emit_false_is_kept() {
		// Without the flag reaching the operator the read point silently drops to step end.
		let with = lookup_with(&lookup(" with { emit: false, exact: true }")).unwrap();
		assert!(with.exact);
		assert_eq!(with.emit, Emit::Off);
		assert!(
			lookup_with(&lookup(" with { exact: true, emit: false }")).unwrap().exact,
			"key order must not matter"
		);
	}

	#[test]
	fn exact_with_any_other_emit_is_refused_at_the_exact_key() {
		// A re-emit replaces the pinned pairing, so exact with emit on or a delay would promise what it cannot
		// keep.
		for emit in ["", "emit: true, ", "emit: 1s, "] {
			let source = lookup(&format!(" with {{ {emit}exact: true }}"));
			let err = lookup_with(&source).expect_err(&format!("must be refused: {source}"));
			assert_eq!(err.fragment.text(), "exact", "wrong span for: {source}");
			assert!(err.to_string().contains("emit: false"), "the error must say why: {err}");
		}
	}

	#[test]
	fn exact_false_is_accepted_with_any_emit() {
		// Writing the default out must never be an error.
		assert!(!lookup_with(&lookup(" with { emit: true, exact: false }")).unwrap().exact);
	}

	#[test]
	fn exact_takes_only_a_boolean() {
		// A duration or word here is a typo for emit, and reading it as true would pin reads silently.
		assert_eq!(code(&lookup(" with { emit: false, exact: 1s }")), "AST_005");
	}
}
