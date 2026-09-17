// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	operator_with::JoinWith,
	row::{JoinPick, JoinRetention, OperatorRetention},
	sort::{SortDirection, SortKey},
};

use crate::{
	Result,
	ast::{
		ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithKey, AstOperatorWithValue},
		identifier::MaybeQualifiedColumnObject,
	},
	bump::BumpFragment,
	diagnostic::AstError,
	duration::{DurationBound, compile_duration},
	plan::logical::{
		Compiler,
		operator_with::{entries, literal, literal_boolean, unknown_key},
	},
	token::{keyword::Keyword, token::Token},
};

const JOIN_WITH_KEYS: &str = "retention, snapshot, latest, or earliest";

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_join_with(
		with: Option<&AstOperatorWith<'bump>>,
		alias: &BumpFragment<'bump>,
	) -> Result<JoinWith> {
		let mut join = JoinWith::default();
		let mut picked = false;
		let mut column_pick = None;
		for entry in entries(with) {
			match entry.key.word() {
				Some("retention") => join.retention = Some(compile_join_retention(entry)?),
				Some("snapshot") => join.snapshot = boolean(entry)?,
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
		if let Some(entry) = column_pick
			&& join.retention.as_ref().is_some_and(|retention| retention.right.is_some())
		{
			return Err(unknown_key(entry, "a pick by time, or a retention without 'right'"));
		}
		Ok(join)
	}
}

fn boolean(entry: &AstOperatorWithEntry<'_>) -> Result<bool> {
	let token = literal(entry)?;
	literal_boolean(token).ok_or_else(|| {
		AstError::UnexpectedToken {
			expected: "boolean literal 'true' or 'false'".to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into()
	})
}

fn compile_join_retention(entry: &AstOperatorWithEntry<'_>) -> Result<JoinRetention> {
	let Some(AstOperatorWithValue::Block(sides)) = &entry.value else {
		return Err(unknown_key(entry, "a block such as { left: 1h, right: 2h }"));
	};
	let mut retention = JoinRetention {
		left: None,
		right: None,
	};
	for side in sides {
		let (slot, context) = match side.key.word() {
			Some("left") => (&mut retention.left, "the 'left' side"),
			Some("right") => (&mut retention.right, "the 'right' side"),
			_ => return Err(unknown_key(side, "'left' or 'right'")),
		};
		*slot = Some(OperatorRetention {
			duration: compile_duration(literal(side)?, DurationBound::Positive, context)?,
		});
	}
	if retention.left.is_none() && retention.right.is_none() {
		return Err(unknown_key(entry, "at least one of 'left' or 'right'"));
	}
	Ok(retention)
}

fn compile_join_pick(
	entry: &AstOperatorWithEntry<'_>,
	default_direction: SortDirection,
	alias: &BumpFragment<'_>,
) -> Result<Option<JoinPick>> {
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

fn direction(token: &Token<'_>) -> Result<SortDirection> {
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
	use reifydb_core::{operator_with::JoinWith, sort::SortDirection};
	use reifydb_value::value::{datetime::TIME_COLUMN_NAME, duration::Duration};

	use crate::{
		Result,
		ast::{ast::AstJoin, parse_str},
		bump::Bump,
		plan::logical::Compiler,
	};

	fn join_with(source: &str) -> Result<JoinWith> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source)?;
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
		}) = statements[0].first_unchecked().as_join();
		Compiler::compile_join_with(with.as_ref(), alias)
	}

	fn join(clause: &str) -> String {
		format!("inner join {{ from orders }} as o using (id, o.user_id) with {{ {clause} }}")
	}

	fn parse_inner(source: &str) -> (bool, Option<Vec<(String, SortDirection)>>) {
		// Reduces the pick to (column name, direction) pairs so every assertion reads as the ordering the RQL
		// asked for.
		let with = join_with(source).unwrap();
		let pick = with.pick.map(|pick| {
			pick.keys.into_iter().map(|key| (key.column.text().to_string(), key.direction)).collect()
		});
		(with.snapshot, pick)
	}

	fn parse_err(source: &str) -> String {
		join_with(source).expect_err("must be rejected").to_string()
	}

	fn hours(n: i64) -> Duration {
		Duration::from_hours(n).unwrap()
	}

	#[test]
	fn test_inner_join_with_ttl_both_sides() {
		let with = join_with(&join("retention: { left: 1h, right: 2d }")).unwrap();
		let retention = with.retention.expect("expected retention block");
		assert_eq!(retention.left.expect("left side present").duration, hours(1));
		assert_eq!(retention.right.expect("right side present").duration, hours(48));
	}

	#[test]
	fn test_inner_join_with_ttl_only_left() {
		let with = join_with(&join("retention: { left: 10m }")).unwrap();
		let retention = with.retention.expect("expected retention block");
		assert_eq!(retention.left.expect("left present").duration, Duration::from_minutes(10).unwrap());
		assert!(retention.right.is_none(), "right side must be absent when only left is given");
	}

	#[test]
	fn test_left_join_with_ttl_only_right() {
		let with = join_with(
			"left join { from orders } as o using (id, o.user_id) with { retention: { right: 1d } }",
		)
		.unwrap();
		let retention = with.retention.expect("expected retention block");
		assert!(retention.left.is_none());
		assert_eq!(retention.right.unwrap().duration, hours(24));
	}

	#[test]
	fn test_join_with_ttl_empty_body_rejected() {
		assert!(join_with(&join("retention: { }")).is_err(), "expected error for empty join retention body");
	}

	#[test]
	fn test_join_with_old_single_ttl_shorthand_rejected() {
		assert!(
			join_with(&join("retention: 1h")).is_err(),
			"expected error for legacy shorthand: retention on join now requires explicit 'left'/'right' keys"
		);
	}

	#[test]
	fn test_join_with_unknown_side_key_rejected() {
		assert!(
			join_with(&join("retention: { middle: 1h }")).is_err(),
			"expected error for unknown side key in join retention"
		);
	}

	#[test]
	fn test_join_with_ttl_and_snapshot() {
		let with = join_with(&join("retention: { left: 5m }, snapshot: true")).unwrap();
		assert!(with.snapshot, "snapshot flag should still parse alongside per-side retention");
		let retention = with.retention.expect("expected retention");
		assert!(retention.left.is_some());
		assert!(retention.right.is_none());
	}

	#[test]
	fn test_join_with_latest_flag() {
		// `latest: true` must keep meaning a pick by #time.
		let (snapshot, pick) = parse_inner(&join("snapshot: true, latest: true, retention: { left: 10s }"));
		assert!(snapshot, "snapshot must parse alongside latest");
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
	fn test_join_rejects_latest_and_earliest_together() {
		let err = parse_err(&join("latest: { o.total }, earliest: { o.seq }"));
		assert!(err.contains("earliest"), "the rejection must name the clash, got: {err}");
	}

	#[test]
	fn test_join_rejects_a_column_ordered_pick_with_a_right_retention() {
		// The pick can crown a row that seals before one it outranked, which was never stored to fall back to.
		for source in [
			join("latest: { o.total }, retention: { right: 2s }"),
			join("earliest: { o.total }, retention: { right: 2s }"),
			join("latest: { o.total }, retention: { left: 1h, right: 2s }"),
		] {
			let err = parse_err(&source);
			assert!(
				err.contains("right"),
				"the rejection must name the side that cannot be retained, got: {err}"
			);
		}
	}

	#[test]
	fn test_join_accepts_a_column_ordered_pick_without_a_right_retention() {
		// Only the right side clashes, so a left retention must never be swept up by the rejection.
		let (_, pick) = parse_inner(&join("latest: { o.total }, retention: { left: 1h }"));
		assert!(pick.is_some(), "a left retention alone must stay legal beside a column-ordered pick");
	}

	#[test]
	fn test_join_accepts_a_time_ordered_pick_with_a_right_retention() {
		// A pick by time crowns the newest row, which seals last, so no row it outranked can outlive it.
		let (_, pick) = parse_inner(&join("latest: true, retention: { left: 1h, right: 2s }"));
		assert_eq!(
			pick,
			Some(vec![(TIME_COLUMN_NAME.to_string(), SortDirection::Desc)]),
			"latest: true must stay legal beside a right retention"
		);
	}
}
