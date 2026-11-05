// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::operator_with::{ChangesWith, DeleteTo, InsertTo, UpdateTo};

use crate::{
	Result,
	ast::ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithValue},
	error::RqlError,
	plan::logical::{
		Compiler,
		operator_with::{entries, unknown_key},
	},
	token::token::Token,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_changes_with(
		token: &Token<'bump>,
		with: Option<&AstOperatorWith<'bump>>,
	) -> Result<ChangesWith> {
		let mut changes = ChangesWith::default();
		for entry in entries(with) {
			match entry.key.word() {
				Some("insert") => changes.insert = compile_insert(entry)?,
				Some("update") => compile_update(entry, &mut changes)?,
				Some("delete") => changes.delete = compile_delete(entry)?,
				_ => return Err(unknown_key(entry, "insert, update or delete")),
			}
		}
		check_changes(token, &changes)?;
		Ok(changes)
	}
}

fn compile_insert(entry: &AstOperatorWithEntry<'_>) -> Result<InsertTo> {
	match target(entry)? {
		"insert" => Ok(InsertTo::Insert),
		"drop" => Ok(InsertTo::Drop),
		_ => Err(unknown_key(entry, "insert: insert or drop")),
	}
}

fn compile_delete(entry: &AstOperatorWithEntry<'_>) -> Result<DeleteTo> {
	match target(entry)? {
		"delete" => Ok(DeleteTo::Delete),
		"insert" => Ok(DeleteTo::Insert),
		"drop" => Ok(DeleteTo::Drop),
		_ => Err(unknown_key(entry, "delete: delete, insert or drop")),
	}
}

fn compile_update(entry: &AstOperatorWithEntry<'_>, changes: &mut ChangesWith) -> Result<()> {
	changes.update = match target(entry)? {
		"update" => UpdateTo::Update,
		"insert" => UpdateTo::Insert,
		"drop" => UpdateTo::Drop,
		_ => return Err(unknown_key(entry, "update: update, insert or drop")),
	};
	changes.update_on = compile_on(entry)?;
	Ok(())
}

fn target<'a>(entry: &'a AstOperatorWithEntry<'_>) -> Result<&'a str> {
	match &entry.value {
		Some(AstOperatorWithValue::Word(token)) => Ok(token.fragment.text()),
		Some(AstOperatorWithValue::Block(settings)) => {
			let to = settings.iter().find(|setting| setting.key.word() == Some("to"));
			for setting in settings {
				if !matches!(setting.key.word(), Some("to") | Some("on")) {
					return Err(unknown_key(setting, "to or on"));
				}
			}
			match to.and_then(|to| to.value.as_ref()) {
				Some(AstOperatorWithValue::Word(token)) => Ok(token.fragment.text()),
				_ => Err(unknown_key(entry, "a block with `to`, such as { to: insert }")),
			}
		}
		_ => Err(unknown_key(entry, "a word such as drop, or a block with `to`")),
	}
}

fn compile_on(entry: &AstOperatorWithEntry<'_>) -> Result<Vec<String>> {
	let Some(AstOperatorWithValue::Block(settings)) = &entry.value else {
		return Ok(Vec::new());
	};
	let Some(on) = settings.iter().find(|setting| setting.key.word() == Some("on")) else {
		return Ok(Vec::new());
	};
	let Some(AstOperatorWithValue::Block(columns)) = &on.value else {
		return Err(unknown_key(on, "a block of columns, such as on: { slot }"));
	};
	if columns.is_empty() {
		return Err(unknown_key(on, "at least one column in on: { ... }"));
	}
	columns.iter()
		.map(|column| match (column.key.word(), &column.value) {
			(Some(name), None) => Ok(name.to_string()),
			_ => Err(unknown_key(column, "a plain column name")),
		})
		.collect()
}

fn check_changes(token: &Token<'_>, changes: &ChangesWith) -> Result<()> {
	let refuse = |reason: &str| -> Result<()> {
		Err(RqlError::ChangesWithRefused {
			reason: reason.to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into())
	};
	if !changes.update_on.is_empty() && changes.update != UpdateTo::Insert {
		return refuse("`on` needs update `to: insert`, otherwise downstream keeps stale values");
	}
	if changes.insert == InsertTo::Insert
		&& changes.update == UpdateTo::Update
		&& changes.delete == DeleteTo::Delete
	{
		return refuse("every change stays as it is, so the operator would do nothing");
	}
	if changes.insert == InsertTo::Drop && changes.update == UpdateTo::Drop && changes.delete == DeleteTo::Drop {
		return refuse("every change is dropped, so the operator would never emit");
	}
	if changes.update == UpdateTo::Update && changes.insert != InsertTo::Insert {
		return refuse("update `update` needs insert `insert`, otherwise downstream never saw the row");
	}
	if changes.delete == DeleteTo::Delete
		&& (changes.insert != InsertTo::Insert || changes.update != UpdateTo::Update)
	{
		return refuse(
			"delete `delete` needs insert `insert` and update `update`, otherwise downstream holds a different row",
		);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::operator_with::{ChangesWith, DeleteTo, InsertTo, UpdateTo};

	use crate::{
		Result,
		ast::{ast::Ast, parse_str},
		bump::Bump,
		plan::logical::Compiler,
	};

	fn changes_with(source: &str) -> Result<ChangesWith> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source)?;
		let Some(Ast::Changes(changes)) = statements[0].nodes.first() else {
			panic!("expected a changes node in: {source}");
		};
		Compiler::compile_changes_with(&changes.token, changes.with.as_ref())
	}

	fn code(source: &str) -> String {
		changes_with(source).expect_err(&format!("must be refused: {source}")).diagnostic().code
	}

	#[test]
	fn all_thirteen_allowed_configs_compile_to_what_they_say() {
		// Each row is written out by hand, so a swapped knob or a lost `on` cannot pass by computing the same
		// answer.
		let on = || vec!["v".to_string()];
		let table: [(&str, InsertTo, UpdateTo, Vec<String>, DeleteTo); 13] = [
			("{ delete: insert }", InsertTo::Insert, UpdateTo::Update, vec![], DeleteTo::Insert),
			("{ delete: drop }", InsertTo::Insert, UpdateTo::Update, vec![], DeleteTo::Drop),
			(
				"{ update: insert, delete: insert }",
				InsertTo::Insert,
				UpdateTo::Insert,
				vec![],
				DeleteTo::Insert,
			),
			(
				"{ update: insert, delete: drop }",
				InsertTo::Insert,
				UpdateTo::Insert,
				vec![],
				DeleteTo::Drop,
			),
			(
				"{ update: { to: insert, on: { v } }, delete: insert }",
				InsertTo::Insert,
				UpdateTo::Insert,
				on(),
				DeleteTo::Insert,
			),
			(
				"{ update: { to: insert, on: { v } }, delete: drop }",
				InsertTo::Insert,
				UpdateTo::Insert,
				on(),
				DeleteTo::Drop,
			),
			(
				"{ update: drop, delete: insert }",
				InsertTo::Insert,
				UpdateTo::Drop,
				vec![],
				DeleteTo::Insert,
			),
			("{ update: drop, delete: drop }", InsertTo::Insert, UpdateTo::Drop, vec![], DeleteTo::Drop),
			(
				"{ insert: drop, update: insert, delete: insert }",
				InsertTo::Drop,
				UpdateTo::Insert,
				vec![],
				DeleteTo::Insert,
			),
			(
				"{ insert: drop, update: insert, delete: drop }",
				InsertTo::Drop,
				UpdateTo::Insert,
				vec![],
				DeleteTo::Drop,
			),
			(
				"{ insert: drop, update: { to: insert, on: { v } }, delete: insert }",
				InsertTo::Drop,
				UpdateTo::Insert,
				on(),
				DeleteTo::Insert,
			),
			(
				"{ insert: drop, update: { to: insert, on: { v } }, delete: drop }",
				InsertTo::Drop,
				UpdateTo::Insert,
				on(),
				DeleteTo::Drop,
			),
			(
				"{ insert: drop, update: drop, delete: insert }",
				InsertTo::Drop,
				UpdateTo::Drop,
				vec![],
				DeleteTo::Insert,
			),
		];
		for (block, insert, update, update_on, delete) in table {
			let with = changes_with(&format!("changes with {block}"))
				.unwrap_or_else(|_| panic!("must compile: {block}"));
			assert_eq!(
				with,
				ChangesWith {
					insert,
					update,
					update_on,
					delete
				},
				"{block}"
			);
		}
	}

	#[test]
	fn all_eleven_refused_configs_are_refused_at_compile() {
		// Each of these would either do nothing, never emit, or leave downstream holding rows it never saw.
		for block in [
			"{ insert: insert, update: update, delete: delete }",
			"{ update: insert, delete: delete }",
			"{ update: { to: insert, on: { v } }, delete: delete }",
			"{ update: drop, delete: delete }",
			"{ insert: drop, update: update, delete: delete }",
			"{ insert: drop, update: update, delete: insert }",
			"{ insert: drop, update: update, delete: drop }",
			"{ insert: drop, update: insert, delete: delete }",
			"{ insert: drop, update: { to: insert, on: { v } }, delete: delete }",
			"{ insert: drop, update: drop, delete: delete }",
			"{ insert: drop, update: drop, delete: drop }",
		] {
			assert_eq!(code(&format!("changes with {block}")), "OP_004", "{block}");
		}
	}

	#[test]
	fn no_with_block_is_refused_because_it_would_do_nothing() {
		assert_eq!(code("changes"), "OP_004");
	}

	#[test]
	fn on_is_refused_unless_update_becomes_an_insert() {
		// A dropped or kept update with `on` would leave downstream holding stale values.
		for block in [
			"{ update: { to: update, on: { v } }, delete: drop }",
			"{ update: { to: drop, on: { v } }, delete: drop }",
		] {
			assert_eq!(code(&format!("changes with {block}")), "OP_004", "{block}");
		}
	}

	#[test]
	fn malformed_blocks_are_refused_with_a_syntax_error() {
		// A misspelled knob or value must never silently fall back to the default.
		for block in [
			"{ upsert: insert }",
			"{ insert: update, delete: drop }",
			"{ insert: delete, delete: drop }",
			"{ update: delete }",
			"{ delete: update }",
			"{ update: { on: { v } } }",
			"{ update: { to: insert, on: { } } }",
			"{ update: { to: insert, on: v } }",
			"{ update: { to: insert, on: { v: 1 } } }",
			"{ update: { to: insert, when: { v } } }",
		] {
			assert!(changes_with(&format!("changes with {block}")).is_err(), "{block}");
		}
	}

	#[test]
	fn block_and_word_forms_mean_the_same() {
		// The two spellings must not drift apart.
		let word = changes_with("changes with { update: insert, delete: drop }").unwrap();
		let block = changes_with("changes with { update: { to: insert }, delete: { to: drop } }").unwrap();
		assert_eq!(word, block);
	}

	#[test]
	fn knobs_read_in_any_case_like_other_keywords() {
		let upper = changes_with("CHANGES WITH { update: insert, delete: drop }").unwrap();
		assert_eq!(upper.update, UpdateTo::Insert);
		assert_eq!(upper.delete, DeleteTo::Drop);
	}
}
