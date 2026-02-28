// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFrom, AstInsert},
		identifier::{
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedSeriesIdentifier, MaybeQualifiedTableIdentifier, UnresolvedPrimitiveIdentifier,
		},
	},
	bump::BumpBox,
	error::RqlError,
	expression::{AliasExpression, ExpressionCompiler, IdentExpression},
	plan::logical::{
		Compiler, InlineDataNode, InsertDictionaryNode, InsertRingBufferNode, InsertSeriesNode,
		InsertTableNode, LogicalPlan,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_insert(
		&self,
		ast: AstInsert<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let unresolved_target = ast.target;
		let source_ast = BumpBox::into_inner(ast.source);

		let source = match source_ast {
			Ast::From(AstFrom::Inline {
				list,
				..
			}) if list.nodes.iter().any(|n| matches!(n, Ast::Tuple(_))) => {
				let has_inlines = list.nodes.iter().any(|n| matches!(n, Ast::Inline(_)));
				if has_inlines {
					return Err(RqlError::InsertMixedRowTypes {
						fragment: list.token.fragment.to_owned(),
					}
					.into());
				}
				self.compile_positional_tuples(&unresolved_target, list.nodes, tx)?
			}
			other => self.compile_single(other, tx)?,
		};

		self.build_insert_node(unresolved_target, source, tx)
	}

	fn build_insert_node(
		&self,
		unresolved_target: UnresolvedPrimitiveIdentifier<'bump>,
		source: LogicalPlan<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let namespace_name = unresolved_target.namespace.first().map(|n| n.text().to_string());
		let namespace_name_str = namespace_name.as_deref().unwrap_or("default");
		let target_name = unresolved_target.name.text();
		let name = unresolved_target.name;
		let namespace = unresolved_target.namespace;

		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_name(tx, namespace_name_str)? {
			ns.id
		} else {
			let mut target = MaybeQualifiedTableIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertTable(InsertTableNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		};

		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertRingBuffer(InsertRingBufferNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		}

		if self.catalog.find_dictionary_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedDictionaryIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertDictionary(InsertDictionaryNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		}

		if self.catalog.find_series_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedSeriesIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertSeries(InsertSeriesNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		}

		let mut target = MaybeQualifiedTableIdentifier::new(name);
		if !namespace.is_empty() {
			target = target.with_namespace(namespace);
		}
		Ok(LogicalPlan::InsertTable(InsertTableNode {
			target,
			source: BumpBox::new_in(source, self.bump),
		}))
	}

	fn compile_positional_tuples(
		&self,
		target: &UnresolvedPrimitiveIdentifier<'bump>,
		nodes: Vec<Ast<'bump>>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let namespace_name = target.namespace.first().map(|n| n.text().to_string());
		let namespace_name_str = namespace_name.as_deref().unwrap_or("default");
		let target_name = target.name.text();

		let column_names = self.catalog.resolve_column_names(tx, namespace_name_str, target_name)?;

		let mut rows = Vec::with_capacity(nodes.len());
		for node in nodes {
			let tuple = match node {
				Ast::Tuple(t) => t,
				_ => unreachable!("validated to contain only tuples"),
			};
			let tuple_len = tuple.nodes.len();

			if tuple_len != column_names.len() {
				return Err(RqlError::InsertPositionalWrongLength {
					fragment: tuple.token.fragment.to_owned(),
					expected: column_names.len(),
					actual: tuple_len,
					column_names: column_names.clone(),
				}
				.into());
			}

			let mut alias_fields = Vec::with_capacity(tuple_len);
			for (i, value_ast) in tuple.nodes.into_iter().enumerate() {
				let col_name = &column_names[i];
				let value_token_fragment = &value_ast.token().fragment;
				let fragment = Fragment::Statement {
					text: Arc::from(col_name.as_str()),
					line: value_token_fragment.line(),
					column: value_token_fragment.column(),
				};
				let alias = IdentExpression(fragment.clone());
				let expr = ExpressionCompiler::compile(value_ast)?;

				alias_fields.push(AliasExpression {
					alias,
					expression: Box::new(expr),
					fragment,
				});
			}
			rows.push(alias_fields);
		}

		Ok(LogicalPlan::InlineData(InlineDataNode {
			rows,
		}))
	}
}
