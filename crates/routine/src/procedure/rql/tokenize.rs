// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use bumpalo::Bump;
use reifydb_core::value::column::{ColumnWithName, columns::Columns};
use reifydb_rql::token::{token::TokenKind, tokenize};
use reifydb_type::value::r#type::Type;

use crate::{
	procedure::rql::extract_query,
	routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("rql::tokenize"));

pub struct RqlTokenize;

impl Default for RqlTokenize {
	fn default() -> Self {
		Self::new()
	}
}

impl RqlTokenize {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for RqlTokenize {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn attaches_row_metadata(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let query = extract_query(ctx.params, "rql::tokenize")?;

		let bump = Bump::new();
		let tokens = tokenize(&bump, query.as_str())?;

		let mut idx_col: Vec<i32> = Vec::with_capacity(tokens.len());
		let mut line_col: Vec<i32> = Vec::with_capacity(tokens.len());
		let mut column_col: Vec<i32> = Vec::with_capacity(tokens.len());
		let mut kind_col: Vec<String> = Vec::with_capacity(tokens.len());
		let mut value_col: Vec<String> = Vec::with_capacity(tokens.len());

		for (i, token) in tokens.iter().enumerate() {
			let (kind, value) = describe_token(&token.kind, token.value());
			idx_col.push(i as i32);
			line_col.push(token.fragment.line().0 as i32);
			column_col.push(token.fragment.column().0 as i32);
			kind_col.push(kind);
			value_col.push(value);
		}

		Ok(Columns::new(vec![
			ColumnWithName::int4("idx", idx_col),
			ColumnWithName::int4("line", line_col),
			ColumnWithName::int4("column", column_col),
			ColumnWithName::utf8("kind", kind_col),
			ColumnWithName::utf8("value", value_col),
		]))
	}
}

fn describe_token(kind: &TokenKind, text: &str) -> (String, String) {
	match kind {
		TokenKind::EOF => ("EOF".to_string(), String::new()),
		TokenKind::Identifier => ("Identifier".to_string(), text.to_string()),
		TokenKind::Keyword(kw) => ("Keyword".to_string(), format!("{:?}", kw)),
		TokenKind::Literal(lit) => ("Literal".to_string(), format!("{:?}", lit)),
		TokenKind::Operator(op) => ("Operator".to_string(), format!("{:?}", op)),
		TokenKind::Variable => ("Variable".to_string(), text.to_string()),
		TokenKind::Separator(sep) => ("Separator".to_string(), format!("{:?}", sep)),
		TokenKind::SystemColumn => ("SystemColumn".to_string(), text.to_string()),
	}
}
