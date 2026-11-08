// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod apply;
pub mod changes;
pub mod join;
pub mod lookup;
pub mod window;

use reifydb_core::operator_with::{AggregateWith, DistinctWith, Emit};
use reifydb_rql_frontend::{
	ast::ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithValue},
	diagnostic::AstError,
	token::token::{Literal, Token, TokenKind},
};
use reifydb_value::{
	error::{AstErrorKind, Error, TypeError},
	fragment::Fragment,
	value::{duration::Duration, number::parse::parse_primitive_uint},
};

use crate::{
	Result,
	duration::{DurationBound, compile_duration},
	error::RqlError,
	plan::logical::Compiler,
};

#[derive(Debug, Clone)]
pub(crate) struct Declared<T> {
	pub value: T,
	pub fragment: Fragment,
}

impl<T: Copy> Declared<T> {
	pub(crate) fn value_of(declared: &Option<Self>) -> Option<T> {
		declared.as_ref().map(|declared| declared.value)
	}
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_aggregate_with(with: Option<&AstOperatorWith<'bump>>) -> Result<AggregateWith> {
		reject_every_key(with, "no WITH keys: aggregate takes none")?;
		Ok(AggregateWith {})
	}

	pub(crate) fn compile_distinct_with(with: Option<&AstOperatorWith<'bump>>) -> Result<DistinctWith> {
		reject_every_key(with, "no WITH keys: distinct takes none")?;
		Ok(DistinctWith {})
	}
}

fn reject_every_key(with: Option<&AstOperatorWith<'_>>, expected: &str) -> Result<()> {
	match with.and_then(|with| with.entries.first()) {
		Some(entry) => Err(unknown_key(entry, expected)),
		None => Ok(()),
	}
}

pub(crate) fn entries<'a, 'bump>(with: Option<&'a AstOperatorWith<'bump>>) -> &'a [AstOperatorWithEntry<'bump>] {
	with.map(|with| with.entries.as_slice()).unwrap_or(&[])
}

pub(crate) fn unknown_key(entry: &AstOperatorWithEntry<'_>, expected: &str) -> Error {
	AstError::UnexpectedToken {
		expected: expected.to_string(),
		fragment: entry.key.fragment(),
	}
	.into()
}

pub(crate) fn literal<'a, 'bump>(entry: &'a AstOperatorWithEntry<'bump>) -> Result<&'a Token<'bump>> {
	match &entry.value {
		Some(AstOperatorWithValue::Literal(token)) => Ok(token),
		Some(AstOperatorWithValue::Word(token)) => Err(AstError::UnexpectedToken {
			expected: "a literal value".to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into()),
		Some(AstOperatorWithValue::Block(_)) | None => Err(AstError::UnexpectedToken {
			expected: "a literal value".to_string(),
			fragment: entry.key.fragment(),
		}
		.into()),
	}
}

pub(crate) fn is_duration(token: &Token<'_>) -> bool {
	token.kind == TokenKind::Literal(Literal::Duration)
}

pub(crate) fn is_number(token: &Token<'_>) -> bool {
	token.kind == TokenKind::Literal(Literal::Number)
}

pub(crate) fn boolean(entry: &AstOperatorWithEntry<'_>) -> Result<bool> {
	let token = literal(entry)?;
	literal_boolean(token).ok_or_else(|| {
		AstError::UnexpectedToken {
			expected: "boolean literal 'true' or 'false'".to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into()
	})
}

pub(crate) fn exact_or_default(exact: Option<(bool, &AstOperatorWithEntry<'_>)>, emit: Emit) -> Result<bool> {
	match exact {
		None => Ok(emit == Emit::Off),
		Some((false, _)) => Ok(false),
		Some((true, _)) if emit == Emit::Off => Ok(true),
		Some((true, entry)) => Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "'exact: true' together with 'emit: false'".to_string(),
			},
			message: "exact pins a row's pairing to the right side as of the row's own source, and a re-emit \
				  on a right change replaces that pairing with a later one, so exact needs emit: false"
				.to_string(),
			fragment: entry.key.fragment(),
		})),
	}
}

pub(crate) fn literal_boolean(token: &Token<'_>) -> Option<bool> {
	match token.kind {
		TokenKind::Literal(Literal::True) => Some(true),
		TokenKind::Literal(Literal::False) => Some(false),
		_ => None,
	}
}

pub(crate) fn declared_duration(token: &Token<'_>, key: &str, bound: DurationBound) -> Result<Declared<Duration>> {
	Ok(Declared {
		value: compile_duration(token, bound, key)?,
		fragment: token.fragment.to_owned(),
	})
}

pub(crate) fn declared_count(token: &Token<'_>) -> Result<Declared<u64>> {
	let count = if is_number(token) {
		parse_primitive_uint::<u64>(token.fragment.to_owned()).ok()
	} else {
		None
	};
	match count {
		Some(value) => Ok(Declared {
			value,
			fragment: token.fragment.to_owned(),
		}),
		None => Err(AstError::UnexpectedToken {
			expected: "number".to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into()),
	}
}

pub(crate) fn reject_immutable_not_smaller_than_lateness<T: PartialOrd>(
	immutable: Option<&Declared<T>>,
	lateness: Option<&Declared<T>>,
) -> Result<()> {
	let (Some(immutable), Some(lateness)) = (immutable, lateness) else {
		return Ok(());
	};
	if immutable.value < lateness.value {
		return Ok(());
	}
	Err(RqlError::WindowImmutableNotSmallerThanLateness {
		immutable_value: immutable.fragment.text().to_string(),
		lateness_value: lateness.fragment.text().to_string(),
		fragment: immutable.fragment.clone(),
	}
	.into())
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;
	use reifydb_rql_frontend::ast::{ast::Ast, parse_str};

	use crate::plan::logical::Compiler;

	#[test]
	fn an_aggregate_with_a_with_clause_after_its_by_clause_is_rejected() {
		// AGGREGATE takes no WITH clause: the BY clause is the only bound on an aggregate's groups.
		let bump = Bump::new();
		let source = "AGGREGATE { count(value) } BY { slot } WITH { lateness: 1m }";
		let mut result = parse_str(&bump, source).unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Aggregate(aggregate)) = result.nodes.first() else {
			panic!("not aggregate")
		};
		assert!(
			Compiler::compile_aggregate_with(aggregate.with.as_ref()).is_err(),
			"expected error: AGGREGATE takes no WITH clause"
		);
	}

	#[test]
	fn test_distinct_with_retention_duration_only_is_rejected() {
		let bump = Bump::new();
		let source = "DISTINCT { x } WITH { lateness: 1h }";
		let mut result = parse_str(&bump, source).unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Distinct(distinct)) = result.nodes.first() else {
			panic!("Expected Distinct operator");
		};
		assert!(
			Compiler::compile_distinct_with(distinct.with.as_ref()).is_err(),
			"expected error: DISTINCT takes no WITH clause"
		);
	}

	#[test]
	fn test_distinct_with_retention_full_config_is_rejected() {
		let bump = Bump::new();
		let source = "DISTINCT { x } WITH { lateness: 5m, on: updated }";
		let mut result = parse_str(&bump, source).unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Distinct(distinct)) = result.nodes.first() else {
			panic!("Expected Distinct operator");
		};
		assert!(
			Compiler::compile_distinct_with(distinct.with.as_ref()).is_err(),
			"expected error: DISTINCT takes no WITH clause"
		);
	}

	#[test]
	fn test_distinct_with_unknown_key_errors() {
		let bump = Bump::new();
		let source = "DISTINCT { x } WITH { unknown: 'foo' }";
		let mut result = parse_str(&bump, source).unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Distinct(distinct)) = result.nodes.first() else {
			panic!("Expected Distinct operator");
		};
		assert!(
			Compiler::compile_distinct_with(distinct.with.as_ref()).is_err(),
			"expected error for unknown WITH key"
		);
	}
}
