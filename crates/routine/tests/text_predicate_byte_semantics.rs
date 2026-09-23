// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::text::{contains::TextContains, ends_with::TextEndsWith, starts_with::TextStartsWith};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{identity::IdentityId, value_type::ValueType},
};

fn ctx(name: &str, row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn call(
	routine: &dyn Routine<FunctionContext<'static>>,
	name: &str,
	args: Vec<ColumnBuffer>,
) -> Result<Columns, RoutineError> {
	let row_count = args.first().map_or(0, |a| a.len());
	let columns = Columns::new(
		args.into_iter()
			.enumerate()
			.map(|(i, data)| ColumnWithName::new(Fragment::internal(format!("arg{i}")), data))
			.collect(),
	);
	routine.call(&mut ctx(name, row_count), &columns)
}

fn answers(name: &str, haystacks: &[&str], needles: &[&str]) -> Vec<String> {
	let routine: Box<dyn Routine<FunctionContext<'static>>> = match name {
		"text::contains" => Box::new(TextContains::new()),
		"text::starts_with" => Box::new(TextStartsWith::new()),
		"text::ends_with" => Box::new(TextEndsWith::new()),
		other => panic!("unknown text predicate {other}"),
	};
	let result = call(
		routine.as_ref(),
		name,
		vec![ColumnBuffer::utf8(haystacks.to_vec()), ColumnBuffer::utf8(needles.to_vec())],
	)
	.unwrap();
	(0..result[0].len()).map(|i| result[0].get_value(i).to_string()).collect()
}

#[test]
fn text_predicates_over_a_multi_byte_column_match_by_bytes_not_characters() {
	// A character based matcher would miss a needle that only lines up on a byte boundary inside a code point run.
	let haystacks = ["café au lait", "naïve", "日本語テキスト", "ascii"];
	let contains_needles = ["é a", "ïve", "本語", "sci"];

	assert_eq!(answers("text::contains", &haystacks, &contains_needles), vec!["true"; 4]);

	let expected: Vec<String> =
		haystacks.iter().zip(contains_needles.iter()).map(|(h, n)| h.contains(n).to_string()).collect();
	assert_eq!(answers("text::contains", &haystacks, &contains_needles), expected);

	assert_eq!(answers("text::starts_with", &haystacks, &["café", "naï", "日本", "asc"]), vec!["true"; 4]);
	assert_eq!(answers("text::ends_with", &haystacks, &["lait", "ïve", "テキスト", "cii"]), vec!["true"; 4]);
}

#[test]
fn text_predicates_answer_false_when_a_needle_only_matches_as_characters() {
	// Stripping accents or folding case would turn these into true and hide a byte level mismatch.
	let haystacks = ["café", "café", "café"];

	assert_eq!(answers("text::contains", &haystacks, &["cafe", "CAFÉ", "e"]), vec!["false"; 3]);
}

#[test]
fn an_empty_pattern_is_true_for_every_row_in_all_three_predicates() {
	// An empty needle is a prefix, a suffix and a substring of every row, including an empty row.
	let haystacks = ["", "a", "日本語"];
	let needles = ["", "", ""];

	assert_eq!(answers("text::contains", &haystacks, &needles), vec!["true"; 3]);
	assert_eq!(answers("text::starts_with", &haystacks, &needles), vec!["true"; 3]);
	assert_eq!(answers("text::ends_with", &haystacks, &needles), vec!["true"; 3]);
}

#[test]
fn a_predicate_over_a_nullable_column_with_no_none_rows_keeps_the_column_nullable() {
	// An all valid null buffer must survive the strip and reattach, otherwise the declared type loses its option.
	let nullable = ColumnBuffer::utf8(["abc", "abd"]).with_nulls(NullBuffer::new(BooleanBuffer::new_set(2)));
	assert_eq!(nullable.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));

	let result =
		call(&TextContains::new(), "text::contains", vec![nullable, ColumnBuffer::utf8(["ab", "ab"])]).unwrap();

	assert_eq!(result[0].get_type(), ValueType::Option(Box::new(ValueType::Boolean)));
	assert_eq!(result[0].get_value(0).to_string(), "true");
	assert_eq!(result[0].get_value(1).to_string(), "true");
}

#[test]
fn a_predicate_over_two_non_nullable_columns_answers_a_non_nullable_boolean() {
	// A spurious null buffer on the result would change the declared type to an option and the wire bytes with it.
	let result = call(
		&TextContains::new(),
		"text::contains",
		vec![ColumnBuffer::utf8(["abc", "abd"]), ColumnBuffer::utf8(["ab", "zz"])],
	)
	.unwrap();

	assert_eq!(result[0].get_type(), ValueType::Boolean);
	assert_eq!(result[0].get_value(0).to_string(), "true");
	assert_eq!(result[0].get_value(1).to_string(), "false");
}
