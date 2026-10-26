// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{from_many, int4, int8, none_typed, utf8};
use reifydb_routine::function::text::{
	ascii::TextAscii, char::TextChar, concat::TextConcat, contains::TextContains, count::TextCount,
	ends_with::TextEndsWith, format_bytes::FormatBytes, format_bytes_si::FormatBytesSi, index_of::TextIndexOf,
	length::TextLength, lower::TextLower, pad_left::TextPadLeft, pad_right::TextPadRight, repeat::TextRepeat,
	replace::TextReplace, reverse::TextReverse, starts_with::TextStartsWith, substring::TextSubstring,
	trim::TextTrim, trim_end::TextTrimEnd, trim_start::TextTrimStart, upper::TextUpper,
};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, column_view::ColumnView, identity::IdentityId},
};

const ROWS: usize = 3;

type Column = (FieldRef, ArrayRef);

struct Case {
	name: &'static str,
	function: Box<dyn Function>,
	columns: Vec<Column>,
	constants: Vec<Value>,
}

fn case(name: &'static str, function: impl Function + 'static, columns: Vec<Column>, constants: Vec<Value>) -> Case {
	// Every position needs a varying column and a constant, otherwise a position goes unchecked.
	assert_eq!(columns.len(), constants.len(), "{name}");
	assert!(columns.iter().all(|(_, array)| array.len() == ROWS), "{name}");
	Case {
		name,
		function: Box::new(function),
		columns,
		constants,
	}
}

fn ctx(name: &str, row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn arg(position: usize) -> String {
	format!("arg{position}")
}

fn constant(value: &Value, position: usize, rows: usize) -> Column {
	from_many(&arg(position), value.clone(), rows)
}

fn none_constant(value: &Value, position: usize, rows: usize) -> Column {
	let value_type = ColumnView::try_from(&constant(value, position, 1)).unwrap().get_type();
	none_typed(&arg(position), value_type, rows)
}

fn compare(case: &Case, label: &str, short: Vec<Column>, full: Vec<Column>, wrong: &mut Vec<String>) {
	// A one row constant must answer exactly what the same value copied to every row answers.
	let short = case.function.call(&mut ctx(case.name, ROWS), &short);
	let full = case.function.call(&mut ctx(case.name, ROWS), &full);
	match (short, full) {
		(Ok((short_field, short)), Ok((full_field, full))) => {
			if short.len() != ROWS {
				wrong.push(format!("{} {label}: {} rows, expected {ROWS}", case.name, short.len()));
			} else if short_field != full_field || short.to_data() != full.to_data() {
				wrong.push(format!("{} {label}: {short:?} differs from {full:?}", case.name));
			}
		}
		(short, full) => wrong.push(format!("{} {label}: {short:?} vs {full:?}", case.name)),
	}
}

fn check(case: &Case, wrong: &mut Vec<String>) {
	for (position, value) in case.constants.iter().enumerate() {
		let replaced = |column: Column| {
			let mut args = case.columns.clone();
			args[position] = column;
			args
		};
		compare(
			case,
			&format!("constant at {position}"),
			replaced(constant(value, position, 1)),
			replaced(constant(value, position, ROWS)),
			wrong,
		);
		compare(
			case,
			&format!("none constant at {position}"),
			replaced(none_constant(value, position, 1)),
			replaced(none_constant(value, position, ROWS)),
			wrong,
		);
	}
	let all = |rows: usize| -> Vec<Column> {
		case.constants.iter().enumerate().map(|(position, value)| constant(value, position, rows)).collect()
	};
	compare(case, "every argument constant", all(1), all(ROWS), wrong);
}

fn words(position: usize, values: [&str; ROWS]) -> Column {
	utf8(&arg(position), values)
}

fn ints(position: usize, values: [i32; ROWS]) -> Column {
	int4(&arg(position), values)
}

fn cases() -> Vec<Case> {
	let text = || vec![words(0, [" Ab ", "cD ", "\u{e9}F"])];
	let hello = || vec![Value::utf8(" Hello W\u{f6}rld ")];
	vec![
		case("text::ascii", TextAscii::new(), text(), hello()),
		case("text::count", TextCount::new(), text(), hello()),
		case("text::length", TextLength::new(), text(), hello()),
		case("text::lower", TextLower::new(), text(), hello()),
		case("text::upper", TextUpper::new(), text(), hello()),
		case("text::trim", TextTrim::new(), text(), hello()),
		case("text::trim_end", TextTrimEnd::new(), text(), hello()),
		case("text::trim_start", TextTrimStart::new(), text(), hello()),
		case("text::reverse", TextReverse::new(), text(), hello()),
		case("text::char", TextChar::new(), vec![ints(0, [65, 98, 233])], vec![Value::int4(72)]),
		case(
			"text::format_bytes",
			FormatBytes::new(),
			vec![int8(&arg(0), [1i64, 2048, 5_000_000])],
			vec![Value::Int8(1536)],
		),
		case(
			"text::format_bytes",
			FormatBytes::new(),
			vec![int8(&arg(0), [1i64, 2048, 5_000_000])],
			vec![Value::float8(1536.5)],
		),
		case(
			"text::format_bytes",
			FormatBytes::new(),
			vec![int8(&arg(0), [1i64, 2048, 5_000_000])],
			vec![Value::Decimal("2048.7".parse().unwrap())],
		),
		case(
			"text::format_bytes_si",
			FormatBytesSi::new(),
			vec![int8(&arg(0), [1i64, 2048, 5_000_000])],
			vec![Value::Int8(1536)],
		),
		case(
			"text::format_bytes_si",
			FormatBytesSi::new(),
			vec![int8(&arg(0), [1i64, 2048, 5_000_000])],
			vec![Value::float8(1536.5)],
		),
		case(
			"text::format_bytes_si",
			FormatBytesSi::new(),
			vec![int8(&arg(0), [1i64, 2048, 5_000_000])],
			vec![Value::Decimal("2048.7".parse().unwrap())],
		),
		case(
			"text::concat",
			TextConcat::new(),
			vec![words(0, ["a", "b", "c"]), words(1, ["x", "y", "z"]), words(2, ["1", "2", "3"])],
			vec![Value::utf8("pre"), Value::utf8("-"), Value::utf8("post")],
		),
		case(
			"text::contains",
			TextContains::new(),
			vec![words(0, ["hello", "world", "help"]), words(1, ["he", "ld", "x"])],
			vec![Value::utf8("hello world"), Value::utf8("l")],
		),
		case(
			"text::starts_with",
			TextStartsWith::new(),
			vec![words(0, ["hello", "world", "help"]), words(1, ["he", "wo", "x"])],
			vec![Value::utf8("hello world"), Value::utf8("h")],
		),
		case(
			"text::ends_with",
			TextEndsWith::new(),
			vec![words(0, ["hello", "world", "help"]), words(1, ["lo", "ld", "x"])],
			vec![Value::utf8("hello world"), Value::utf8("d")],
		),
		case(
			"text::index_of",
			TextIndexOf::new(),
			vec![words(0, ["hello", "world", "help"]), words(1, ["l", "or", "x"])],
			vec![Value::utf8("hello world"), Value::utf8("l")],
		),
		case(
			"text::replace",
			TextReplace::new(),
			vec![words(0, ["aXa", "bXb", "cc"]), words(1, ["X", "b", "c"]), words(2, ["-", "+", "*"])],
			vec![Value::utf8("XaX"), Value::utf8("a"), Value::utf8("_")],
		),
		case(
			"text::repeat",
			TextRepeat::new(),
			vec![words(0, ["a", "bc", "d"]), ints(1, [0, 2, 3])],
			vec![Value::utf8("ab"), Value::int4(2)],
		),
		case(
			"text::pad_left",
			TextPadLeft::new(),
			vec![words(0, ["a", "bcd", "ef"]), ints(1, [3, 2, 5]), words(2, ["*", "-", "xy"])],
			vec![Value::utf8("z"), Value::int4(4), Value::utf8(".")],
		),
		case(
			"text::pad_right",
			TextPadRight::new(),
			vec![words(0, ["a", "bcd", "ef"]), ints(1, [3, 2, 5]), words(2, ["*", "-", "xy"])],
			vec![Value::utf8("z"), Value::int4(4), Value::utf8(".")],
		),
		case(
			"text::substring",
			TextSubstring::new(),
			vec![words(0, ["hello", "world", "abc"]), ints(1, [0, 1, -2]), ints(2, [2, 3, 1])],
			vec![Value::utf8("constant"), Value::int4(1), Value::int4(3)],
		),
		case(
			"text::substring",
			TextSubstring::new(),
			vec![
				words(0, ["hello", "world", "abc"]),
				int8(&arg(1), [0i64, 1, -2]),
				int8(&arg(2), [2i64, 3, 1]),
			],
			vec![Value::utf8("constant"), Value::Int8(1), Value::Int8(3)],
		),
	]
}

#[test]
fn every_text_function_answers_a_one_row_constant_like_the_constant_copied_to_every_row() {
	// Reading a one row argument at row i past zero answers an empty string or a short column instead of the
	// constant.
	let mut wrong = Vec::new();
	for case in cases() {
		check(&case, &mut wrong);
	}
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
