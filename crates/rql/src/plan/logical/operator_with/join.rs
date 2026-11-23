// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	operator_with::{Emit, JoinWith},
	row::{JoinPick, JoinSpans},
	sort::{SortDirection, SortKey},
};
use reifydb_rql_frontend::{
	ast::{
		ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithKey, AstOperatorWithValue},
		identifier::MaybeQualifiedColumnObject,
	},
	bump::BumpFragment,
	diagnostic::AstError,
	token::{keyword::Keyword, token::Token},
};
use reifydb_value::error::{AstErrorKind, TypeError, ValueError};

use crate::{
	duration::{DurationBound, compile_duration},
	plan::logical::{
		Compiler,
		operator_with::{boolean, entries, exact_or_default, literal, literal_boolean, unknown_key},
	},
};

const JOIN_WITH_KEYS: &str = "drop, forget, emit, exact, latest, or earliest";

fn compile_join_emit(entry: &AstOperatorWithEntry<'_>) -> Result<Emit, ValueError> {
	let token = literal(entry)?;
	literal_boolean(token).map(Emit::from).ok_or_else(|| {
		AstError::UnexpectedToken {
			expected: "'true' or 'false'; a duration is only supported on lookup".to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into()
	})
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_join_with(
		with: Option<&AstOperatorWith<'bump>>,
		alias: &BumpFragment<'bump>,
	) -> Result<JoinWith, ValueError> {
		let mut join = JoinWith::default();
		let mut picked = false;
		let mut column_pick = None;
		let mut exact = None;
		let mut forget_key = None;
		for entry in entries(with) {
			match entry.key.word() {
				Some("drop") => join.drop = Some(compile_join_spans(entry)?),
				Some("forget") => {
					join.forget = Some(compile_join_spans(entry)?);
					forget_key = Some(entry.key.fragment());
				}
				Some("emit") => join.emit = compile_join_emit(entry)?,
				Some("exact") => exact = Some((boolean(entry)?, entry)),
				Some(keyword @ ("latest" | "earliest")) => {
					if picked {
						return Err(unknown_key(entry, "one of 'latest' or 'earliest'"));
					}
					picked = true;
					let direction = match keyword {
						"latest" => SortDirection::Desc,
						_ => SortDirection::Asc,
					};
					if matches!(&entry.value, Some(AstOperatorWithValue::Block(columns)) if !columns.is_empty())
					{
						column_pick = Some(entry);
					}
					join.pick = compile_join_pick(entry, direction, alias)?;
				}
				_ => return Err(unknown_key(entry, JOIN_WITH_KEYS)),
			}
		}
		join.exact = exact_or_default(exact, join.emit)?;
		if let Err(mut err) = join.check_forget() {
			if let Some(fragment) = forget_key {
				err.with_fragment(fragment);
			}
			return Err(err);
		}
		if let Some(entry) = column_pick
			&& join.effective_drop_right().is_some()
		{
			return Err(ValueError::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "a pick by time, or a forget without 'right'".to_string(),
				},
				message: "a column-ordered pick holds one right row per key, and a right forget can free \
					  that row while a row it outranked is still live, leaving the join with nothing to \
					  fall back to"
					.to_string(),
				fragment: entry.key.fragment(),
			}));
		}
		Ok(join)
	}
}

fn compile_join_spans(entry: &AstOperatorWithEntry<'_>) -> Result<JoinSpans, ValueError> {
	let Some(AstOperatorWithValue::Block(sides)) = &entry.value else {
		return Err(unknown_key(entry, "a block such as { left: 1h, right: 2h }"));
	};
	let mut spans = JoinSpans {
		left: None,
		right: None,
	};
	for side in sides {
		let (slot, context) = match side.key.word() {
			Some("left") => (&mut spans.left, "the 'left' side"),
			Some("right") => (&mut spans.right, "the 'right' side"),
			_ => return Err(unknown_key(side, "'left' or 'right'")),
		};
		*slot = Some(compile_duration(literal(side)?, DurationBound::Positive, context)?);
	}
	if spans.left.is_none() && spans.right.is_none() {
		return Err(unknown_key(entry, "at least one of 'left' or 'right'"));
	}
	Ok(spans)
}

fn compile_join_pick(
	entry: &AstOperatorWithEntry<'_>,
	default_direction: SortDirection,
	alias: &BumpFragment<'_>,
) -> Result<Option<JoinPick>, ValueError> {
	let columns = match &entry.value {
		Some(AstOperatorWithValue::Block(columns)) => columns,
		_ => {
			return Ok(match boolean(entry)? {
				true => Some(JoinPick::by_time(default_direction)),
				false => None,
			});
		}
	};
	if columns.is_empty() {
		return Ok(Some(JoinPick::by_time(default_direction)));
	}

	let mut keys = Vec::with_capacity(columns.len());
	for column in columns {
		let name = match &column.key {
			AstOperatorWithKey::Word(token) => token.fragment.to_owned(),
			AstOperatorWithKey::Column(column) => {
				let owned = match &column.object {
					MaybeQualifiedColumnObject::Unqualified => true,
					MaybeQualifiedColumnObject::Alias(name) => name.text() == alias.text(),
					MaybeQualifiedColumnObject::Qualified {
						name,
						..
					} => name.text() == alias.text(),
				};
				if !owned {
					return Err(AstError::UnexpectedToken {
						expected: format!(
							"a column of '{}': the pick chooses among right rows only",
							alias.text()
						),
						fragment: column.name.to_owned(),
					}
					.into());
				}
				column.name.to_owned()
			}
		};
		let direction = match &column.value {
			None => default_direction.clone(),
			Some(AstOperatorWithValue::Word(token)) => direction(token)?,
			Some(AstOperatorWithValue::Literal(token)) => direction(token)?,
			Some(AstOperatorWithValue::Block(_)) => {
				return Err(unknown_key(column, "'asc' or 'desc' after the column"));
			}
		};
		keys.push(SortKey {
			column: name,
			direction,
		});
	}

	Ok(Some(JoinPick {
		keys,
	}))
}

fn direction(token: &Token<'_>) -> Result<SortDirection, ValueError> {
	if token.is_keyword(Keyword::Asc) {
		Ok(SortDirection::Asc)
	} else if token.is_keyword(Keyword::Desc) {
		Ok(SortDirection::Desc)
	} else {
		Err(AstError::UnexpectedToken {
			expected: "'asc' or 'desc'".to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		operator_with::{Emit, JoinWith},
		sort::SortDirection,
	};
	use reifydb_rql_frontend::{
		ast::{
			ast::{Ast, AstJoin},
			parse_str,
		},
		bump::Bump,
	};
	use reifydb_value::{
		error::ValueError,
		value::{datetime::TIME_COLUMN_NAME, duration::Duration},
	};

	use crate::plan::logical::Compiler;

	fn join_with(source: &str) -> Result<JoinWith, ValueError> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source)?;
		let Some(Ast::Join(join)) = statements[0].nodes.first() else {
			panic!("not join")
		};
		let (AstJoin::InnerJoin {
			with,
			alias,
			..
		}
		| AstJoin::LeftJoin {
			with,
			alias,
			..
		}
		| AstJoin::NaturalJoin {
			with,
			alias,
			..
		}) = &**join;
		Compiler::compile_join_with(with.as_ref(), alias)
	}

	fn join(clause: &str) -> String {
		format!("inner join {{ from orders }} as o using (id, o.user_id) with {{ {clause} }}")
	}

	fn parse_inner(source: &str) -> (Emit, Option<Vec<(String, SortDirection)>>) {
		// Reduces the pick to (column name, direction) pairs so every assertion reads as the ordering the RQL
		// asked for.
		let with = join_with(source).unwrap();
		let pick = with.pick.map(|pick| {
			pick.keys.into_iter().map(|key| (key.column.text().to_string(), key.direction)).collect()
		});
		(with.emit, pick)
	}

	fn parse_err(source: &str) -> String {
		join_with(source).expect_err("must be rejected").to_string()
	}

	fn hours(n: i64) -> Duration {
		Duration::from_hours(n).unwrap()
	}

	#[test]
	fn test_inner_join_with_ttl_both_sides() {
		let with = join_with(&join("forget: { left: 1h, right: 2d }")).unwrap();
		let forget = with.forget.expect("expected forget block");
		assert_eq!(forget.left.expect("left side present"), hours(1));
		assert_eq!(forget.right.expect("right side present"), hours(48));
	}

	#[test]
	fn test_inner_join_with_ttl_only_left() {
		let with = join_with(&join("forget: { left: 10m }")).unwrap();
		let forget = with.forget.expect("expected forget block");
		assert_eq!(forget.left.expect("left present"), Duration::from_minutes(10).unwrap());
		assert!(forget.right.is_none(), "right side must be absent when only left is given");
	}

	#[test]
	fn test_left_join_with_ttl_only_right() {
		let with = join_with(
			"left join { from orders } as o using (id, o.user_id) with { forget: { right: 1d } }",
		)
		.unwrap();
		let forget = with.forget.expect("expected forget block");
		assert!(forget.left.is_none());
		assert_eq!(forget.right.unwrap(), hours(24));
	}

	#[test]
	fn a_join_parses_drop_and_forget_independently_per_side() {
		// Each block must fill only its own field and side, otherwise a drop silently frees rows or a forget
		// cuts arrivals.
		let with = join_with(&join("drop: { right: 10m }, forget: { left: 1h }")).unwrap();
		let drop_spans = with.drop.clone().expect("drop block present");
		let forget = with.forget.clone().expect("forget block present");
		assert_eq!(drop_spans.left, None);
		assert_eq!(drop_spans.right, Some(Duration::from_minutes(10).unwrap()));
		assert_eq!(forget.left, Some(hours(1)));
		assert_eq!(forget.right, None);
		assert_eq!(with.effective_drop_left(), Some(hours(1)), "a side without drop must cut at its forget");
		assert_eq!(with.effective_drop_right(), Some(Duration::from_minutes(10).unwrap()));

		let drop_only = join_with(&join("drop: { left: 5m }")).unwrap();
		assert!(drop_only.forget.is_none(), "a drop alone must never free stored rows");
		let forget_only = join_with(&join("forget: { left: 5m }")).unwrap();
		assert!(forget_only.drop.is_none(), "a forget alone must leave the drop block unset");
	}

	#[test]
	fn a_join_forget_below_the_drop_of_its_side_is_refused_at_the_forget_key() {
		// Stored rows must outlive the arrivals still accepted for them, and the span must name the key to fix.
		for clause in [
			"drop: { left: 1h }, forget: { left: 10m }",
			"forget: { right: 10m }, drop: { right: 1h }",
			"drop: { left: 1m, right: 1h }, forget: { left: 1h, right: 10m }",
		] {
			let err = join_with(&join(clause)).expect_err(&format!("must be refused: {clause}"));
			assert_eq!(err.code, "FLOW_081", "wrong diagnostic for: {clause}");
			assert_eq!(err.fragment.text(), "forget", "wrong span for: {clause}");
		}
		assert!(join_with(&join("drop: { left: 1h }, forget: { left: 1h }")).is_ok(), "equal must pass");
		assert!(
			join_with(&join("drop: { left: 1h }, forget: { right: 10m }")).is_ok(),
			"the check is per side, so a short forget on the other side must pass"
		);
	}

	#[test]
	fn test_join_with_ttl_empty_body_rejected() {
		assert!(join_with(&join("forget: { }")).is_err(), "expected error for empty join forget body");
	}

	#[test]
	fn test_join_with_old_single_ttl_shorthand_rejected() {
		assert!(
			join_with(&join("forget: 1h")).is_err(),
			"expected error for legacy shorthand: forget on join now requires explicit 'left'/'right' keys"
		);
	}

	#[test]
	fn test_join_with_unknown_side_key_rejected() {
		assert!(
			join_with(&join("forget: { middle: 1h }")).is_err(),
			"expected error for unknown side key in join forget"
		);
	}

	#[test]
	fn test_join_with_ttl_and_emit() {
		let with = join_with(&join("forget: { left: 5m }, emit: false")).unwrap();
		assert_eq!(with.emit, Emit::Off, "emit should still parse alongside per-side forget");
		let forget = with.forget.expect("expected forget");
		assert!(forget.left.is_some());
		assert!(forget.right.is_none());
	}

	#[test]
	fn test_join_with_latest_flag() {
		// `latest: true` must keep meaning a pick by #time.
		let (emit, pick) = parse_inner(&join("emit: false, latest: true, forget: { left: 10s }"));
		assert_eq!(emit, Emit::Off, "emit must parse alongside latest");
		assert_eq!(
			pick,
			Some(vec![(TIME_COLUMN_NAME.to_string(), SortDirection::Desc)]),
			"latest: true must pick by #time"
		);
	}

	#[test]
	fn test_join_latest_defaults_false() {
		let (_, pick) = parse_inner("inner join { from orders } as o using (id, o.user_id)");
		assert_eq!(pick, None, "a join with no with clause must not pick");
	}

	#[test]
	fn test_join_latest_defaults_each_column_to_descending() {
		// `latest` means the greatest, so a bare column must not silently order ascending and return the
		// smallest row.
		let (_, pick) = parse_inner(&join("latest: { o.total }"));
		assert_eq!(pick, Some(vec![("total".to_string(), SortDirection::Desc)]));
	}

	#[test]
	fn test_join_earliest_defaults_each_column_to_ascending() {
		let (_, pick) = parse_inner(&join("earliest: { o.total }"));
		assert_eq!(pick, Some(vec![("total".to_string(), SortDirection::Asc)]));
	}

	#[test]
	fn test_join_pick_mixes_directions_across_columns() {
		// "Greatest total, ties to the smallest seq" is unsayable when one keyword fixes every column's
		// direction.
		let (_, pick) = parse_inner(&join("latest: { o.total, o.seq: asc }"));
		assert_eq!(
			pick,
			Some(
				vec![
					("total".to_string(), SortDirection::Desc),
					("seq".to_string(), SortDirection::Asc),
				]
			),
			"an explicit direction must override the keyword default, and only for its own column"
		);
	}

	#[test]
	fn test_join_pick_binds_four_columns_in_written_order() {
		let (_, pick) = parse_inner(&join("earliest: { o.a, o.b: desc, o.c, o.d: desc }"));
		assert_eq!(
			pick,
			Some(vec![
				("a".to_string(), SortDirection::Asc),
				("b".to_string(), SortDirection::Desc),
				("c".to_string(), SortDirection::Asc),
				("d".to_string(), SortDirection::Desc),
			]),
			"lexicographic order is written order, and each column keeps its own direction"
		);
	}

	#[test]
	fn test_join_explicit_direction_equals_the_mirrored_keyword() {
		// Two spellings of one ordering must produce the same pick, or a view silently changes meaning when
		// rewritten.
		let (_, explicit) = parse_inner(&join("latest: { o.total: asc }"));
		let (_, keyword) = parse_inner(&join("earliest: { o.total }"));
		assert_eq!(explicit, keyword);
	}

	#[test]
	fn test_join_empty_pick_braces_equal_the_boolean_form() {
		let (_, braces) = parse_inner(&join("latest: { }"));
		let (_, boolean) = parse_inner(&join("latest: true"));
		assert_eq!(braces, boolean, "an empty brace list and the boolean form must be the same pick");
	}

	#[test]
	fn test_join_latest_false_is_no_pick() {
		let (_, pick) = parse_inner(&join("latest: false"));
		assert_eq!(pick, None, "latest: false must leave the join an ordinary fan-out");
	}

	#[test]
	fn test_join_pick_rejects_a_direction_that_is_not_asc_or_desc() {
		let err = parse_err(&join("latest: { o.total: sideways }"));
		assert!(err.contains("asc"), "the rejection must name what is allowed, got: {err}");
	}

	#[test]
	fn a_zero_side_forget_is_refused() {
		// Zero must never reach the join, otherwise it reads as keep forever while a window reads it as no late
		// rows.
		assert!(join_with(&join("forget: { left: 0s }")).is_err());
		assert!(join_with(&join("forget: { right: 0s }")).is_err());
		assert!(join_with(&join("forget: { left: 1s }")).is_ok());
	}

	#[test]
	fn test_join_rejects_latest_and_earliest_together() {
		let err = parse_err(&join("latest: { o.total }, earliest: { o.seq }"));
		assert!(err.contains("earliest"), "the rejection must name the clash, got: {err}");
	}

	#[test]
	fn test_join_rejects_a_column_ordered_pick_with_a_right_forget() {
		// The pick can crown a row that seals before one it outranked, which was never stored to fall back to.
		for source in [
			join("latest: { o.total }, forget: { right: 2s }"),
			join("earliest: { o.total }, forget: { right: 2s }"),
			join("latest: { o.total }, forget: { left: 1h, right: 2s }"),
		] {
			let err = parse_err(&source);
			assert!(
				err.contains("right"),
				"the rejection must name the side that cannot be kept, got: {err}"
			);
		}
	}

	#[test]
	fn test_join_rejects_a_column_ordered_pick_with_a_right_drop() {
		// A right drop cuts the same rows a right forget frees, so it must never slip past the pick rule.
		for source in [
			join("latest: { o.total }, drop: { right: 2s }"),
			join("earliest: { o.total }, drop: { left: 1h, right: 2s }"),
		] {
			let err = join_with(&source).expect_err(&format!("must be refused: {source}"));
			assert_eq!(err.code, "AST_005", "wrong diagnostic for: {source}");
			assert!(err.to_string().contains("right"), "the rejection must name the side, got: {err}");
		}
		assert!(
			join_with(&join("latest: { o.total }, drop: { left: 1h }")).is_ok(),
			"a left drop alone must stay legal beside a column-ordered pick"
		);
	}

	#[test]
	fn test_join_accepts_a_column_ordered_pick_without_a_right_forget() {
		// Only the right side clashes, so a left forget must never be swept up by the rejection.
		let (_, pick) = parse_inner(&join("latest: { o.total }, forget: { left: 1h }"));
		assert!(pick.is_some(), "a left forget alone must stay legal beside a column-ordered pick");
	}

	#[test]
	fn test_join_accepts_a_time_ordered_pick_with_a_right_forget() {
		// A pick by time crowns the newest row, which seals last, so no row it outranked can outlive it.
		let (_, pick) = parse_inner(&join("latest: true, forget: { left: 1h, right: 2s }"));
		assert_eq!(
			pick,
			Some(vec![(TIME_COLUMN_NAME.to_string(), SortDirection::Desc)]),
			"latest: true must stay legal beside a right forget"
		);
	}

	#[test]
	fn a_join_refuses_a_duration_for_emit_and_names_lookup() {
		// a join has no delay, so accepting one would silently mean something else.
		let message = parse_err(&join("emit: 1s"));
		assert!(message.contains("lookup"), "{message}");
	}

	#[test]
	fn test_join_emit_off_is_exact_unless_it_opts_out() {
		// Exact is the safe default, so folding an emit-off join must be written out as exact: false.
		assert!(join_with(&join("emit: false, latest: true")).unwrap().exact);
		assert!(!join_with(&join("emit: false, exact: false, latest: true")).unwrap().exact);
		assert!(!join_with("inner join { from orders } as o using (id, o.user_id)").unwrap().exact);
	}

	#[test]
	fn test_join_exact_with_emit_false_is_kept() {
		// Without the flag the analyzer folds the join's backlog and pairs at step end.
		let with = join_with(&join("emit: false, exact: true, latest: true")).unwrap();
		assert!(with.exact);
		assert_eq!(with.emit, Emit::Off);
	}

	#[test]
	fn test_join_exact_without_emit_false_is_refused() {
		// A join that re-publishes on a right change cannot keep a pinned pairing, so exact alone must fail.
		for clause in ["exact: true", "emit: true, exact: true"] {
			let message = parse_err(&join(clause));
			assert!(message.contains("emit: false"), "the error must say why for {clause}: {message}");
		}
	}
}
