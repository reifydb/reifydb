// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, render::DefaultRenderer},
	fragment::Fragment,
};

const RQL: &str = "let $f = ($a, $b) { $a + $b }; $f(1)";

fn diagnostic(fragment: Fragment, cause: Option<Diagnostic>) -> Diagnostic {
	Diagnostic {
		code: "FUNCTION_002".to_string(),
		rql: Some(RQL.to_string()),
		message: "function $f expects 2 arguments, got 1".to_string(),
		fragment,
		label: Some("wrong number of arguments".to_string()),
		cause: cause.map(Box::new),
		..Default::default()
	}
}

fn underline_start(rendered: &str) -> usize {
	let lines: Vec<&str> = rendered.lines().collect();
	let source = lines
		.iter()
		.position(|line| line.ends_with(RQL))
		.unwrap_or_else(|| panic!("the source line must be rendered, got:\n{rendered}"));
	let underline = lines[source + 1];
	let tilde = underline
		.chars()
		.position(|c| c == '~')
		.unwrap_or_else(|| panic!("an underline must follow the source line, got:\n{rendered}"));
	let text_start = lines[source].chars().count() - RQL.chars().count();
	tilde - text_start
}

#[test]
fn the_flat_underline_sits_under_the_copy_at_the_reported_column() {
	// The call at column 32 must be underlined, never the earlier `let $f` that a text search finds first.
	let rendered = DefaultRenderer::render_string(&diagnostic(Fragment::statement("$f", 1, 32), None));
	assert_eq!(underline_start(&rendered), 31, "the underline must start under the call, got:\n{rendered}");
}

#[test]
fn the_nested_underline_sits_under_the_copy_at_the_reported_column() {
	// An error with a cause renders through the nested path, which must place the underline the same way.
	let cause = Diagnostic {
		code: "CAUSE".to_string(),
		message: "inner".to_string(),
		..Default::default()
	};
	let rendered = DefaultRenderer::render_string(&diagnostic(Fragment::statement("$f", 1, 32), Some(cause)));
	assert_eq!(underline_start(&rendered), 31, "the underline must start under the call, got:\n{rendered}");
}

#[test]
fn a_column_pointing_at_other_text_falls_back_to_the_searched_copy() {
	// A column that does not hold the fragment text is unreliable, so the underline must go to the text itself.
	let rendered = DefaultRenderer::render_string(&diagnostic(Fragment::statement("$f", 1, 1), None));
	assert_eq!(underline_start(&rendered), 4, "the underline must start under the first copy, got:\n{rendered}");
}

#[test]
fn text_missing_from_the_line_is_underlined_at_the_one_based_column() {
	// Columns count from one, so a fragment absent from the line must start at column minus one, not one past it.
	let rendered = DefaultRenderer::render_string(&diagnostic(Fragment::statement("zz", 1, 5), None));
	assert_eq!(underline_start(&rendered), 4, "the underline must start at the column, got:\n{rendered}");
}

#[test]
fn an_empty_fragment_renders_no_indent_on_the_underline_line() {
	// Every line starts with empty text, so an empty fragment must never count as matching at the column.
	let rql = "map { cast(\"\", datetime) }";
	let diagnostic = Diagnostic {
		code: "CAST_002".to_string(),
		rql: Some(rql.to_string()),
		message: "failed to cast".to_string(),
		fragment: Fragment::statement("", 1, 13),
		label: Some("failed to cast to DateTime".to_string()),
		..Default::default()
	};
	let rendered = DefaultRenderer::render_string(&diagnostic);
	let lines: Vec<&str> = rendered.lines().collect();
	let source = lines
		.iter()
		.position(|line| line.ends_with(rql))
		.unwrap_or_else(|| panic!("the source line must be rendered, got:\n{rendered}"));
	assert_eq!(lines[source + 1], "    │ ", "an empty fragment must add no indent, got:\n{rendered}");
}

#[test]
fn a_flat_diagnostic_without_a_label_renders_no_padded_blank_line() {
	// An empty label must drop the line entirely; padding it to the fragment column writes trailing whitespace into every golden.
	let mut d = diagnostic(Fragment::statement("$f", 1, 32), None);
	d.label = None;
	let rendered = DefaultRenderer::render_string(&d);
	assert!(
		!rendered.lines().any(|line| line.ends_with(' ')),
		"no rendered line may end in whitespace, got:\n{rendered:?}"
	);
}

#[test]
fn a_nested_diagnostic_without_a_label_renders_no_padded_blank_line() {
	// The nested path already guards this; the assertion pins both paths to the same shape.
	let cause = Diagnostic {
		code: "CAUSE".to_string(),
		message: "inner".to_string(),
		..Default::default()
	};
	let mut d = diagnostic(Fragment::statement("$f", 1, 32), Some(cause));
	d.label = None;
	let rendered = DefaultRenderer::render_string(&d);
	assert!(
		!rendered.lines().any(|line| line.ends_with(' ')),
		"no rendered line may end in whitespace, got:\n{rendered:?}"
	);
}
