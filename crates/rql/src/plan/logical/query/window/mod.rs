// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_value::{
	fragment::Fragment,
	value::{duration::Duration, number::parse::parse_primitive_int, temporal::parse::duration::parse_duration},
};

use crate::{
	Result,
	ast::ast::{
		Ast,
		Ast::Literal,
		AstLiteral::{Number, Text},
		AstWindow, AstWindowConfig, AstWindowKind,
	},
	bump::BumpFragment,
	diagnostic::AstError,
	error::RqlError,
	expression::{Expression, ExpressionCompiler},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone)]
struct Declared<T> {
	pub value: T,
	pub fragment: Fragment,
}

impl<T: Copy> Declared<T> {
	fn value_of(declared: &Option<Self>) -> Option<T> {
		declared.as_ref().map(|declared| declared.value)
	}
}

#[derive(Debug, Default)]
struct ParsedConfig {
	pub interval: Option<Declared<Duration>>,
	pub count: Option<Declared<u64>>,
	pub slide_duration: Option<Declared<Duration>>,
	pub slide_count: Option<Declared<u64>>,
	pub gap: Option<Declared<Duration>>,
	pub lag: Option<Declared<Duration>>,
	pub seal: Option<Declared<Duration>>,
	pub window: Fragment,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub seal: Duration,
	pub rql: String,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_window(&self, ast: AstWindow<'bump>) -> Result<LogicalPlan<'bump>> {
		let rql = ast.rql.to_string();

		let parsed = Self::parse_config(&ast.config, ast.token.fragment.to_owned())?;
		let group_by = Self::compile_expressions(ast.group_by)?;
		let aggregations = Self::compile_expressions(ast.aggregations)?;
		let kind = Self::build_window_kind(ast.kind, &parsed)?;
		Self::reject_time_only_config(&parsed, &kind)?;

		Ok(LogicalPlan::Window(WindowNode {
			kind,
			group_by,
			aggregations,
			seal: Declared::value_of(&parsed.seal).unwrap_or_default(),
			rql,
		}))
	}

	fn reject_time_only_config(parsed: &ParsedConfig, kind: &WindowKind) -> Result<()> {
		if !kind.size().is_some_and(|size| size.is_count()) {
			return Ok(());
		}
		if let Some(declared) = &parsed.seal {
			return Err(AstError::UnexpectedToken {
				expected: "no seal on count-based windows (seal needs a time domain)".to_string(),
				fragment: declared.fragment.clone(),
			}
			.into());
		}
		Ok(())
	}

	fn declared_slide(parsed: &ParsedConfig) -> Option<Fragment> {
		parsed.slide_duration
			.as_ref()
			.map(|declared| declared.fragment.clone())
			.or_else(|| parsed.slide_count.as_ref().map(|declared| declared.fragment.clone()))
	}

	fn declared_size(parsed: &ParsedConfig) -> Option<Fragment> {
		parsed.interval
			.as_ref()
			.map(|declared| declared.fragment.clone())
			.or_else(|| parsed.count.as_ref().map(|declared| declared.fragment.clone()))
	}

	fn declared_text(fragment: Option<&Fragment>) -> String {
		fragment.map(|fragment| fragment.text().to_string()).unwrap_or_default()
	}

	fn reject_slide_in_a_different_domain(
		parsed: &ParsedConfig,
		size: &WindowSize,
		slide: &WindowSize,
	) -> Result<()> {
		let domain = |measure: &WindowSize| match measure {
			WindowSize::Duration(_) => "interval",
			WindowSize::Count(_) => "count",
		};
		if domain(size) == domain(slide) {
			return Ok(());
		}
		let slide_declared = Self::declared_slide(parsed);
		Err(RqlError::WindowIncompatibleSlideType {
			window_type: domain(size).to_string(),
			slide_type: domain(slide).to_string(),
			fragment: slide_declared.unwrap_or_else(|| parsed.window.clone()),
		}
		.into())
	}

	fn reject_slide_of_zero(parsed: &ParsedConfig, slide: &WindowSize) -> Result<()> {
		let zero = match slide {
			WindowSize::Duration(slide) => slide.is_zero(),
			WindowSize::Count(slide) => *slide == 0,
		};
		if !zero {
			return Ok(());
		}
		let slide_declared = Self::declared_slide(parsed);
		Err(RqlError::WindowSlideNotPositive {
			window_value: Self::declared_text(Self::declared_size(parsed).as_ref()),
			fragment: slide_declared.unwrap_or_else(|| parsed.window.clone()),
		}
		.into())
	}

	fn reject_slide_not_smaller_than_size(
		parsed: &ParsedConfig,
		size: &WindowSize,
		slide: &WindowSize,
	) -> Result<()> {
		let too_large = match (size, slide) {
			(WindowSize::Duration(size), WindowSize::Duration(slide)) => slide >= size,
			(WindowSize::Count(size), WindowSize::Count(slide)) => slide >= size,
			_ => false,
		};
		if !too_large {
			return Ok(());
		}
		let slide_declared = Self::declared_slide(parsed);
		Err(RqlError::WindowSlideTooLarge {
			slide_value: Self::declared_text(slide_declared.as_ref()),
			window_value: Self::declared_text(Self::declared_size(parsed).as_ref()),
			fragment: slide_declared.unwrap_or_else(|| parsed.window.clone()),
		}
		.into())
	}

	#[inline]
	fn parse_config(config: &[AstWindowConfig<'bump>], window: Fragment) -> Result<ParsedConfig> {
		let mut parsed = ParsedConfig {
			window,
			..Default::default()
		};
		for config_item in config {
			Self::parse_config_item(config_item, &mut parsed)?;
		}
		Ok(parsed)
	}

	fn compile_expressions(asts: Vec<Ast<'bump>>) -> Result<Vec<Expression>> {
		let mut expressions = Vec::new();
		for ast in asts {
			expressions.push(ExpressionCompiler::compile(ast)?);
		}
		Ok(expressions)
	}

	#[inline]
	fn build_window_kind(kind: AstWindowKind, parsed: &ParsedConfig) -> Result<WindowKind> {
		if let Some(lag) = parsed.lag.as_ref()
			&& !matches!(kind, AstWindowKind::Rolling)
		{
			return Err(AstError::UnexpectedToken {
				expected: "lag is only supported for rolling windows".to_string(),
				fragment: lag.fragment.clone(),
			}
			.into());
		}

		match kind {
			AstWindowKind::Tumbling => {
				let size = Self::build_measure(parsed)?;
				Ok(WindowKind::Tumbling {
					size,
				})
			}
			AstWindowKind::Sliding => {
				let size = Self::build_measure(parsed)?;
				let slide = if let Some(d) = parsed.slide_duration.as_ref() {
					WindowSize::Duration(d.value)
				} else if let Some(c) = parsed.slide_count.as_ref() {
					WindowSize::Count(c.value)
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "slide parameter is required for sliding windows".to_string(),
						fragment: parsed.window.clone(),
					}
					.into());
				};
				Self::reject_slide_in_a_different_domain(parsed, &size, &slide)?;
				Self::reject_slide_of_zero(parsed, &slide)?;
				Self::reject_slide_not_smaller_than_size(parsed, &size, &slide)?;
				Ok(WindowKind::Sliding {
					size,
					slide,
				})
			}
			AstWindowKind::Rolling => {
				let size = Self::build_measure(parsed)?;
				if let Some(lag) = parsed.lag.as_ref()
					&& !matches!(size, WindowSize::Duration(_))
				{
					return Err(AstError::UnexpectedToken {
						expected: "lag is only supported with a duration interval".to_string(),
						fragment: lag.fragment.clone(),
					}
					.into());
				}
				Ok(WindowKind::Rolling {
					size,
					lag: Declared::value_of(&parsed.lag),
				})
			}
			AstWindowKind::Session => {
				let gap = parsed.gap.as_ref().ok_or_else(|| AstError::UnexpectedToken {
					expected: "gap parameter is required for session windows".to_string(),
					fragment: parsed.window.clone(),
				})?;
				Ok(WindowKind::Session {
					gap: gap.value,
				})
			}
		}
	}

	fn build_measure(parsed: &ParsedConfig) -> Result<WindowSize> {
		if let Some(d) = parsed.interval.as_ref() {
			Ok(WindowSize::Duration(d.value))
		} else if let Some(c) = parsed.count.as_ref() {
			Ok(WindowSize::Count(c.value))
		} else {
			Err(AstError::UnexpectedToken {
				expected: "interval or count must be specified".to_string(),
				fragment: parsed.window.clone(),
			}
			.into())
		}
	}

	fn parse_config_item(config_item: &AstWindowConfig<'bump>, config: &mut ParsedConfig) -> Result<()> {
		match config_item.key.text() {
			"interval" | "duration" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.interval = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"count" => {
				if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.count = Some(Declared {
						value: count_val as u64,
						fragment: config_item.value.token().fragment.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "number".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"slide" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.slide_duration = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.slide_count = Some(Declared {
						value: count_val as u64,
						fragment: config_item.value.token().fragment.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string or number".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"gap" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.gap = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"lag" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.lag = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"seal" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.seal = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			_ => {
				return Err(AstError::UnexpectedToken {
					expected: "interval, count, slide, gap, lag, or seal".to_string(),
					fragment: config_item.key.token.fragment.to_owned(),
				}
				.into());
			}
		}
		Ok(())
	}

	pub fn extract_text_fragment(ast: &Ast<'bump>) -> Option<BumpFragment<'bump>> {
		if let Literal(literal) = ast
			&& let Text(text) = literal
		{
			Some(text.0.fragment)
		} else {
			None
		}
	}

	pub fn extract_literal_number(ast: &Ast) -> Option<i64> {
		if let Literal(literal) = ast
			&& let Number(number) = literal
		{
			parse_primitive_int::<i64>(number.0.fragment.to_owned()).ok()
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{ast::parse_str, bump::Bump};

	fn parse_window_config(source: &str) -> Result<ParsedConfig> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source).unwrap();
		let window = statements[0].first_unchecked().as_window();
		Compiler::parse_config(&window.config, window.token.fragment.to_owned())
	}

	#[test]
	fn a_rejected_key_points_at_that_key() {
		// A `with { }` block may hold a dozen keys, so a failure without a span leaves the author nothing to
		// act on. This asserts the span, not just the rejection.
		let parsed = parse_window_config(r#"window tumbling { count(*) } with { interval: "5m", lag: "30s" }"#)
			.unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Tumbling, &parsed).unwrap_err();
		assert_eq!(err.fragment.text(), "30s", "the offending lag value is what the author must remove");
	}

	#[test]
	fn a_missing_measure_points_at_the_window() {
		// An error about something absent has no key to point at, so it has to fall back to the window token;
		// otherwise the author cannot tell which window in the statement lacks the measure.
		let parsed = parse_window_config(r#"window tumbling { count(*) } with { seal: "1s" }"#).unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Tumbling, &parsed).unwrap_err();
		assert_eq!(err.fragment.text(), "window", "the window token is the fallback span");
	}

	#[test]
	fn a_slide_at_least_as_large_as_the_window_is_rejected() {
		// A slide equal to the size is a tumbling window, and a larger slide leaves coordinates covered by no
		// window at all - the operator has no defined answer for a row in one of those gaps and silently
		// assigns it to the preceding window.
		for source in [
			r#"window sliding { count(*) } with { interval: "1m", slide: "5m" }"#,
			r#"window sliding { count(*) } with { interval: "1m", slide: "1m" }"#,
			r#"window sliding { count(*) } with { count: 10, slide: 10 }"#,
			r#"window sliding { count(*) } with { count: 10, slide: 25 }"#,
		] {
			let parsed = parse_window_config(source).unwrap();
			let err = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed)
				.expect_err(&format!("a non-overlapping slide must be rejected: {source}"));
			assert_eq!(err.diagnostic().code, "WINDOW_003", "wrong diagnostic for: {source}");
		}
	}

	#[test]
	fn a_zero_slide_is_rejected_before_it_can_divide_by_zero() {
		// Every sliding anchor computation divides by the slide, so a zero slide panics inside the operator
		// and takes the flow down. The `slide >= size` guard does not catch it, since `0 >= size` is false for
		// every real size, and both domains divide.
		for source in [
			r#"window sliding { count(*) } with { interval: "5m", slide: "0s" }"#,
			r#"window sliding { count(*) } with { count: 10, slide: 0 }"#,
		] {
			let parsed = parse_window_config(source).unwrap();
			let err = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed)
				.expect_err(&format!("a zero slide divides by zero and must be refused: {source}"));
			assert_eq!(err.diagnostic().code, "WINDOW_008", "wrong diagnostic for: {source}");
		}
	}

	#[test]
	fn a_zero_slide_is_not_reported_as_a_slide_that_is_too_large() {
		// "must be smaller than the window interval" is true of a zero slide, so it tells the author to do
		// what they already did. Shrink-the-slide and make-it-positive are different fixes and need different
		// diagnostics.
		let parsed = parse_window_config(r#"window sliding { count(*) } with { interval: "5m", slide: "0s" }"#)
			.unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed).unwrap_err();
		assert_ne!(err.diagnostic().code, "WINDOW_003", "a zero slide is not a slide that is too large");
	}

	#[test]
	fn a_slide_measured_in_a_different_unit_than_the_window_is_rejected() {
		// `interval` buckets by event time and `count` by arrival ordinal, so a window cannot be sized in one
		// and advanced in the other. A mixed pair misses both arms of the anchor mapping and puts every row
		// of the flow in window 0 - one window, never sealed, unbounded, with no error anywhere.
		for source in [
			r#"window sliding { count(*) } with { interval: "5m", slide: 3 }"#,
			r#"window sliding { count(*) } with { count: 100, slide: "1m" }"#,
		] {
			let parsed = parse_window_config(source).unwrap();
			let err = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed)
				.expect_err(&format!("a slide in the wrong domain must be refused: {source}"));
			assert_eq!(err.diagnostic().code, "WINDOW_004", "wrong diagnostic for: {source}");
		}
	}

	#[test]
	fn a_mismatched_slide_points_at_the_slide_the_author_declared() {
		// The author has to change the slide to match the window's unit, so the span must land on the slide
		// value rather than the window token.
		let parsed = parse_window_config(r#"window sliding { count(*) } with { interval: "5m", slide: 3 }"#)
			.unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed).unwrap_err();
		assert_eq!(err.fragment.text(), "3", "the offending slide value is what the author must change");
	}

	#[test]
	fn a_rejected_slide_points_at_the_slide_value() {
		// Two durations sit in the block and only the slide is at fault, so pointing anywhere else leaves the
		// author guessing which one to change.
		let parsed = parse_window_config(r#"window sliding { count(*) } with { interval: "1m", slide: "5m" }"#)
			.unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed).unwrap_err();
		assert_eq!(err.fragment.text(), "5m", "the offending slide value is what the author must reduce");
	}

	#[test]
	fn an_overlapping_slide_is_still_accepted() {
		// The control for the rejection tests above: a validation that refused everything would look correct
		// without it, and a slide smaller than the size is what sliding exists for.
		let parsed = parse_window_config(r#"window sliding { count(*) } with { interval: "5m", slide: "1m" }"#)
			.unwrap();

		let kind = Compiler::<'static>::build_window_kind(AstWindowKind::Sliding, &parsed)
			.expect("a slide smaller than the size is the overlapping case sliding exists for");
		assert!(matches!(kind, WindowKind::Sliding { .. }));
	}

	#[test]
	fn unknown_with_key_still_rejected() {
		// `with { }` accepts only keys the engine reads; a removed knob that is still accepted is a
		// declaration the author believes is in force.
		assert!(parse_window_config(r#"window tumbling { count(*) } with { bogus: 1 }"#).is_err());
		assert!(
			parse_window_config(
				r#"window tumbling { count(*) } with { interval: "5m", state_cache_size: 4096 }"#
			)
			.is_err(),
			"state_cache_size was removed and must not be silently accepted"
		);
		assert!(
			parse_window_config(
				r#"window tumbling { count(*) } with { interval: "5m", internal_state_cache_size: 512 }"#
			)
			.is_err(),
			"internal_state_cache_size was removed and must not be silently accepted"
		);
		// One admissible span cannot be two numbers: seal is the whole allowance, so accepting lateness
		// again lets a user widen a horizon the seal gate does not honour.
		assert!(
			parse_window_config(r#"window tumbling { count(*) } with { interval: "5m", lateness: "30s" }"#)
				.is_err(),
			"lateness was removed and must not be silently accepted"
		);
	}
}
