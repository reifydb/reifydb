// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use bumpalo::Bump;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::{
	column::Column,
	id::{ColumnId, NamespaceId, TableId},
	relationship::{Relationship, RelationshipCardinality, RelationshipJunction},
	table::Table,
};
use reifydb_rql::{
	ast::{
		ast::{
			Ast, AstAppend, AstAppendSource, AstBlock, AstFilter, AstFor, AstFrom, AstInfix, AstInline,
			AstInlineKeyedValue, AstLet, AstList, AstLiteral, AstLiteralBoolean, AstLiteralNumber,
			AstLiteralText, AstMap, AstSkip, AstStatement, AstSubQuery, AstTake, AstTakeValue, AstVariable,
			InfixOperator, LetValue,
		},
		identifier::{UnqualifiedIdentifier, UnresolvedShapeIdentifier},
	},
	bump::{BumpBox, BumpFragment},
	token::token::{Literal as RqlLiteral, Token as RqlToken, TokenKind as RqlTokenKind},
};
use reifydb_transaction::transaction::Transaction;
use thiserror::Error;

use crate::{
	ast::ast::*,
	token::token::{Token as GqlToken, TokenKind as GqlTokenKind},
};

#[derive(Error, Debug)]
pub enum CompilerError {
	#[error("Unsupported operation: {0}")]
	UnsupportedOperation(String),
	#[error("Top-level selection must be a field")]
	TopLevelSelectionNotField,
	#[error("Unknown root field '{0}'")]
	UnknownRootField(String),
	#[error("Unknown relationship '{field}' on table '{table}'")]
	UnknownRelationship {
		field: String,
		table: String,
	},
	#[error("Catalog error: {0}")]
	Catalog(String),
	#[error("Column id {0:?} not found on table '{1}'")]
	ColumnNotFound(ColumnId, String),
	#[error("Junction table id {0:?} not found")]
	JunctionTableNotFound(TableId),
}

pub struct Compiler<'bump> {
	bump: &'bump Bump,
	catalog: Catalog,
}

impl<'bump> Compiler<'bump> {
	pub fn new(bump: &'bump Bump, catalog: Catalog) -> Self {
		Self {
			bump,
			catalog,
		}
	}

	pub fn compile(
		&self,
		tx: &mut Transaction<'_>,
		operation: &AstOperation<'bump>,
	) -> Result<AstStatement<'bump>, CompilerError> {
		let root_field = match operation.selections.first() {
			Some(AstSelection::Field(field)) => field,
			_ => return Err(CompilerError::TopLevelSelectionNotField),
		};

		let namespace_id = NamespaceId::DEFAULT;
		let root_name = root_field.name.text().to_string();
		let root_table = self
			.catalog
			.find_table_by_name(tx, namespace_id, &root_name)
			.map_err(|e| CompilerError::Catalog(format!("{e}")))?
			.ok_or_else(|| CompilerError::UnknownRootField(root_name.clone()))?;

		let mut counter: usize = 0;
		let level = self.build_level(tx, namespace_id, root_field, &root_table, None, &mut counter)?;

		let mut nodes: Vec<Ast<'bump>> = Vec::new();

		self.emit_level_lets(&level, &mut nodes)?;

		let rows_let = self.let_empty_list("rows", root_field.token);
		nodes.push(Ast::Let(rows_let));

		let for_node = self.emit_for(&level, root_field.token)?;
		nodes.push(Ast::For(for_node));

		nodes.push(Ast::Variable(self.make_variable("rows", root_field.token)));

		Ok(AstStatement {
			nodes,
			has_pipes: false,
			is_output: false,
			rql: "",
		})
	}

	fn build_level<'a>(
		&self,
		tx: &mut Transaction<'_>,
		namespace_id: NamespaceId,
		field: &'a AstField<'bump>,
		table: &Table,
		relation: Option<ResolvedRelation>,
		counter: &mut usize,
	) -> Result<LevelNode<'a, 'bump>, CompilerError> {
		let index = *counter;
		*counter += 1;

		let mut leaves: Vec<&'a AstField<'bump>> = Vec::new();
		let mut children: Vec<LevelNode<'a, 'bump>> = Vec::new();

		if let Some(selections) = &field.selections {
			for selection in selections {
				let AstSelection::Field(child_field) = selection;
				if child_field.selections.is_some() {
					let child_name = child_field.name.text();
					let rel = self
						.catalog
						.find_relationship_by_name(tx, namespace_id, table.id, child_name)
						.map_err(|e| CompilerError::Catalog(format!("{e}")))?
						.ok_or_else(|| CompilerError::UnknownRelationship {
							field: child_name.to_string(),
							table: table.name.clone(),
						})?;
					let resolved = self.resolve_relation(tx, table, &rel)?;
					let target_table = self
						.catalog
						.find_table(tx, rel.target_table)
						.map_err(|e| CompilerError::Catalog(format!("{e}")))?
						.ok_or_else(|| {
							CompilerError::Catalog(format!(
								"target table {:?} not found for relationship '{}'",
								rel.target_table, rel.name
							))
						})?;
					let child_node = self.build_level(
						tx,
						namespace_id,
						child_field,
						&target_table,
						Some(resolved),
						counter,
					)?;
					children.push(child_node);
				} else {
					leaves.push(child_field);
				}
			}
		}

		Ok(LevelNode {
			index,
			field,
			table_name: table.name.clone(),
			relation,
			leaves,
			children,
		})
	}

	fn resolve_relation(
		&self,
		tx: &mut Transaction<'_>,
		parent_table: &Table,
		rel: &Relationship,
	) -> Result<ResolvedRelation, CompilerError> {
		let source_column_name = column_name_or_err(parent_table, rel.source_column)?;

		let target_table = self
			.catalog
			.find_table(tx, rel.target_table)
			.map_err(|e| CompilerError::Catalog(format!("{e}")))?
			.ok_or_else(|| {
				CompilerError::Catalog(format!(
					"target table {:?} not found for relationship '{}'",
					rel.target_table, rel.name
				))
			})?;
		let target_column_name = column_name_or_err(&target_table, rel.target_column)?;

		let junction = match &rel.junction {
			Some(j) => Some(self.resolve_junction(tx, j)?),
			None => None,
		};

		Ok(ResolvedRelation {
			cardinality: rel.cardinality,
			source_column_name,
			target_column_name,
			junction,
		})
	}

	fn resolve_junction(
		&self,
		tx: &mut Transaction<'_>,
		j: &RelationshipJunction,
	) -> Result<ResolvedJunction, CompilerError> {
		let table = self
			.catalog
			.find_table(tx, j.table)
			.map_err(|e| CompilerError::Catalog(format!("{e}")))?
			.ok_or(CompilerError::JunctionTableNotFound(j.table))?;
		let source_column_name = column_name_or_err(&table, j.source_column)?;
		let target_column_name = column_name_or_err(&table, j.target_column)?;
		Ok(ResolvedJunction {
			table_name: table.name.clone(),
			source_column_name,
			target_column_name,
		})
	}

	fn emit_level_lets<'a>(
		&self,
		level: &LevelNode<'a, 'bump>,
		nodes: &mut Vec<Ast<'bump>>,
	) -> Result<(), CompilerError> {
		nodes.push(Ast::Let(self.build_level_let(level)?));

		for child in &level.children {
			if let Some(rel) = &child.relation
				&& let Some(j) = &rel.junction
			{
				nodes.push(Ast::Let(self.build_junction_let(child, rel, j)?));
			}
			self.emit_level_lets(child, nodes)?;
		}

		Ok(())
	}

	fn build_level_let<'a>(&self, level: &LevelNode<'a, 'bump>) -> Result<AstLet<'bump>, CompilerError> {
		let level_name = format!("level{}", level.index);
		let var_token = self.make_variable_token(&level_name, level.field.token);
		let name = UnqualifiedIdentifier::new(var_token);
		let let_token = self.to_rql_token(level.field.token);

		let table_name = self.bump.alloc_str(&level.table_name);
		let table_fragment = BumpFragment::Internal {
			text: table_name,
		};

		let from_node = Ast::From(AstFrom::Source {
			token: let_token,
			source: UnresolvedShapeIdentifier {
				namespace: Vec::new(),
				name: table_fragment,
				alias: None,
			},
			index_name: None,
		});

		let mut pipeline_nodes: Vec<Ast<'bump>> = vec![from_node];

		if let Some(rel) = &level.relation {
			let parent_index = level.index - 1;
			let parent_level_name = format!("level{}", parent_index);
			let parent_var = self.make_variable(&parent_level_name, level.field.token);

			match (rel.cardinality, &rel.junction) {
				(RelationshipCardinality::ManyToMany, Some(j)) => {
					let junction_var_name = format!("junction{}", level.index);
					let junction_var = self.make_variable(&junction_var_name, level.field.token);
					let target_col_ident = self.identifier_ast(&rel.target_column_name);
					let in_subquery = self.subquery_var_pipe_map(
						junction_var,
						&j.target_column_name,
						level.field.token,
					);
					pipeline_nodes.push(Ast::Filter(self.in_filter(
						target_col_ident,
						in_subquery,
						level.field.token,
					)));
				}
				_ => {
					let target_col_ident = self.identifier_ast(&rel.target_column_name);
					let in_subquery = self.subquery_var_pipe_map(
						parent_var,
						&rel.source_column_name,
						level.field.token,
					);
					pipeline_nodes.push(Ast::Filter(self.in_filter(
						target_col_ident,
						in_subquery,
						level.field.token,
					)));
				}
			}
		}

		self.append_field_arguments(&mut pipeline_nodes, level.field)?;

		let statement = AstStatement {
			nodes: pipeline_nodes,
			has_pipes: true,
			is_output: false,
			rql: "",
		};

		Ok(AstLet {
			token: let_token,
			name,
			value: LetValue::Statement(statement),
		})
	}

	fn build_junction_let<'a>(
		&self,
		level: &LevelNode<'a, 'bump>,
		rel: &ResolvedRelation,
		j: &ResolvedJunction,
	) -> Result<AstLet<'bump>, CompilerError> {
		let var_name = format!("junction{}", level.index);
		let var_token = self.make_variable_token(&var_name, level.field.token);
		let name = UnqualifiedIdentifier::new(var_token);
		let let_token = self.to_rql_token(level.field.token);

		let table_fragment_text = self.bump.alloc_str(&j.table_name);
		let table_fragment = BumpFragment::Internal {
			text: table_fragment_text,
		};
		let from_node = Ast::From(AstFrom::Source {
			token: let_token,
			source: UnresolvedShapeIdentifier {
				namespace: Vec::new(),
				name: table_fragment,
				alias: None,
			},
			index_name: None,
		});

		let parent_index = level.index - 1;
		let parent_level_name = format!("level{}", parent_index);
		let parent_var = self.make_variable(&parent_level_name, level.field.token);

		let junction_source_col_ident = self.identifier_ast(&j.source_column_name);
		let in_subquery = self.subquery_var_pipe_map(parent_var, &rel.source_column_name, level.field.token);
		let filter = Ast::Filter(self.in_filter(junction_source_col_ident, in_subquery, level.field.token));

		let statement = AstStatement {
			nodes: vec![from_node, filter],
			has_pipes: true,
			is_output: false,
			rql: "",
		};

		Ok(AstLet {
			token: let_token,
			name,
			value: LetValue::Statement(statement),
		})
	}

	fn emit_for<'a>(
		&self,
		root_level: &LevelNode<'a, 'bump>,
		token: GqlToken<'bump>,
	) -> Result<AstFor<'bump>, CompilerError> {
		let r0_var_name = format!("r{}", root_level.index);
		let for_var = self.make_variable(&r0_var_name, token);
		let level_var = self.make_variable(&format!("level{}", root_level.index), token);
		let iterable = BumpBox::new_in(Ast::Variable(level_var), self.bump);

		let mut body_statements: Vec<AstStatement<'bump>> = Vec::new();
		for child in &root_level.children {
			self.emit_child_lets(child, &mut body_statements)?;
		}

		let inline = self.build_inline_for_level(root_level)?;
		let list_node = AstList {
			token: self.to_rql_token(token),
			nodes: vec![Ast::Inline(inline)],
		};
		let rows_target = self.make_variable("rows", token);
		let append = AstAppend::IntoVariable {
			token: self.to_rql_token(token),
			target: rows_target,
			source: AstAppendSource::Inline(list_node),
		};
		body_statements.push(AstStatement {
			nodes: vec![Ast::Append(append)],
			has_pipes: false,
			is_output: false,
			rql: "",
		});

		let body = AstBlock {
			token: self.to_rql_token(token),
			statements: body_statements,
		};

		Ok(AstFor {
			token: self.to_rql_token(token),
			variable: for_var,
			iterable,
			body,
		})
	}

	fn emit_child_lets<'a>(
		&self,
		level: &LevelNode<'a, 'bump>,
		body_statements: &mut Vec<AstStatement<'bump>>,
	) -> Result<(), CompilerError> {
		let rel = level.relation.as_ref().expect("non-root level must have a relation");
		let parent_index = level.index - 1;

		if matches!(rel.cardinality, RelationshipCardinality::ManyToMany) && rel.junction.is_some() {
			let junction = rel.junction.as_ref().unwrap();
			body_statements.push(self.let_junction_filter(level, rel, junction, parent_index)?);
		}

		body_statements.push(self.let_row_filter(level, rel, parent_index)?);

		for child in &level.children {
			self.emit_child_lets(child, body_statements)?;
		}
		Ok(())
	}

	fn let_row_filter<'a>(
		&self,
		level: &LevelNode<'a, 'bump>,
		rel: &ResolvedRelation,
		parent_index: usize,
	) -> Result<AstStatement<'bump>, CompilerError> {
		let var_name = format!("r{}", level.index);
		let var_token = self.make_variable_token(&var_name, level.field.token);
		let name = UnqualifiedIdentifier::new(var_token);
		let let_token = self.to_rql_token(level.field.token);

		let source_var = self.make_variable(&format!("level{}", level.index), level.field.token);

		let from_var_node = Ast::From(AstFrom::Variable {
			token: let_token,
			variable: source_var,
		});

		let filter_expr = match (rel.cardinality, &rel.junction) {
			(RelationshipCardinality::ManyToMany, Some(_)) => {
				let junction_var = self.make_variable(&format!("j{}", level.index), level.field.token);
				let in_subquery = self.subquery_var_pipe_map(
					junction_var,
					&rel.junction.as_ref().unwrap().target_column_name,
					level.field.token,
				);
				let target_col = self.identifier_ast(&rel.target_column_name);
				Ast::Filter(self.in_filter(target_col, in_subquery, level.field.token))
			}
			_ => {
				let parent_var = self.make_variable(&format!("r{}", parent_index), level.field.token);
				let parent_col_access =
					self.var_dot_col(parent_var, &rel.source_column_name, level.field.token);
				let target_col = self.identifier_ast(&rel.target_column_name);
				Ast::Filter(self.eq_filter(target_col, parent_col_access, level.field.token))
			}
		};

		let statement = AstStatement {
			nodes: vec![from_var_node, filter_expr],
			has_pipes: true,
			is_output: false,
			rql: "",
		};

		Ok(AstStatement {
			nodes: vec![Ast::Let(AstLet {
				token: let_token,
				name,
				value: LetValue::Statement(statement),
			})],
			has_pipes: false,
			is_output: false,
			rql: "",
		})
	}

	fn let_junction_filter<'a>(
		&self,
		level: &LevelNode<'a, 'bump>,
		rel: &ResolvedRelation,
		junction: &ResolvedJunction,
		parent_index: usize,
	) -> Result<AstStatement<'bump>, CompilerError> {
		let var_name = format!("j{}", level.index);
		let var_token = self.make_variable_token(&var_name, level.field.token);
		let name = UnqualifiedIdentifier::new(var_token);
		let let_token = self.to_rql_token(level.field.token);

		let junction_source_var = self.make_variable(&format!("junction{}", level.index), level.field.token);
		let from_var_node = Ast::From(AstFrom::Variable {
			token: let_token,
			variable: junction_source_var,
		});

		let parent_var = self.make_variable(&format!("r{}", parent_index), level.field.token);
		let parent_col_access = self.var_dot_col(parent_var, &rel.source_column_name, level.field.token);
		let junction_source_col = self.identifier_ast(&junction.source_column_name);
		let filter = Ast::Filter(self.eq_filter(junction_source_col, parent_col_access, level.field.token));

		let statement = AstStatement {
			nodes: vec![from_var_node, filter],
			has_pipes: true,
			is_output: false,
			rql: "",
		};

		Ok(AstStatement {
			nodes: vec![Ast::Let(AstLet {
				token: let_token,
				name,
				value: LetValue::Statement(statement),
			})],
			has_pipes: false,
			is_output: false,
			rql: "",
		})
	}

	fn build_inline_for_level<'a>(&self, level: &LevelNode<'a, 'bump>) -> Result<AstInline<'bump>, CompilerError> {
		let r_var = format!("r{}", level.index);
		let mut keyed_values: Vec<AstInlineKeyedValue<'bump>> = Vec::new();

		for leaf in &level.leaves {
			let key_text = leaf.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| leaf.name.text());
			let key_fragment = BumpFragment::Internal {
				text: self.bump.alloc_str(key_text),
			};
			let key = UnqualifiedIdentifier::from_fragment(key_fragment);

			let var = self.make_variable(&r_var, leaf.token);
			let value_ast = self.var_dot_col(var, leaf.name.text(), leaf.token);
			let value = BumpBox::new_in(value_ast, self.bump);
			keyed_values.push(AstInlineKeyedValue {
				key,
				value,
			});
		}

		for child in &level.children {
			let key_text =
				child.field.alias.as_ref().map(|a| a.text()).unwrap_or_else(|| child.field.name.text());
			let key_fragment = BumpFragment::Internal {
				text: self.bump.alloc_str(key_text),
			};
			let key = UnqualifiedIdentifier::from_fragment(key_fragment);

			let value_ast = match child.relation.as_ref().map(|r| r.cardinality) {
				Some(RelationshipCardinality::OneToMany)
				| Some(RelationshipCardinality::ManyToMany) => Ast::Variable(
					self.make_variable(&format!("r{}", child.index), child.field.token),
				),
				_ => Ast::Inline(self.build_inline_for_level(child)?),
			};
			let value = BumpBox::new_in(value_ast, self.bump);
			keyed_values.push(AstInlineKeyedValue {
				key,
				value,
			});
		}

		Ok(AstInline {
			token: self.to_rql_token(level.field.token),
			keyed_values,
		})
	}

	fn append_field_arguments(
		&self,
		nodes: &mut Vec<Ast<'bump>>,
		field: &AstField<'bump>,
	) -> Result<(), CompilerError> {
		let Some(args) = &field.arguments else {
			return Ok(());
		};
		for arg in args {
			match arg.name.text() {
				"where" => {
					if let AstValue::Object(fields) = &arg.value {
						for f in fields {
							nodes.push(self.compile_where_field(f)?);
						}
					}
				}
				"first" => {
					if let AstValue::Int(token) = &arg.value {
						let val = token.value().parse().unwrap_or(0);
						nodes.push(Ast::Take(AstTake {
							token: self.to_rql_token(arg.token),
							take: AstTakeValue::Literal(val),
						}));
					}
				}
				"skip" => {
					if let AstValue::Int(token) = &arg.value {
						let val = token.value().parse().unwrap_or(0);
						nodes.push(Ast::Skip(AstSkip {
							token: self.to_rql_token(arg.token),
							skip: AstTakeValue::Literal(val),
						}));
					}
				}
				_ => {}
			}
		}
		Ok(())
	}

	fn compile_where_field(&self, field: &AstObjectField<'bump>) -> Result<Ast<'bump>, CompilerError> {
		let name = field.name.text();
		let rql_token = self.to_rql_token(field.token);

		let (column_name, operator) = if let Some(stripped) = name.strip_suffix("_gt") {
			(stripped, InfixOperator::GreaterThan(rql_token))
		} else if let Some(stripped) = name.strip_suffix("_lt") {
			(stripped, InfixOperator::LessThan(rql_token))
		} else if let Some(stripped) = name.strip_suffix("_gte") {
			(stripped, InfixOperator::GreaterThanEqual(rql_token))
		} else if let Some(stripped) = name.strip_suffix("_lte") {
			(stripped, InfixOperator::LessThanEqual(rql_token))
		} else {
			(name, InfixOperator::Equal(rql_token))
		};

		let left = self.identifier_ast(column_name);

		let right = match &field.value {
			AstValue::Int(t) => Ast::Literal(AstLiteral::Number(AstLiteralNumber(self.to_rql_token(*t)))),
			AstValue::String(t) => Ast::Literal(AstLiteral::Text(AstLiteralText(self.to_rql_token(*t)))),
			AstValue::Boolean(t) => {
				Ast::Literal(AstLiteral::Boolean(AstLiteralBoolean(self.boolean_rql_token(*t))))
			}
			_ => return Err(CompilerError::UnsupportedOperation("Complex where values".to_string())),
		};

		let infix = AstInfix {
			token: rql_token,
			left: BumpBox::new_in(left, self.bump),
			operator,
			right: BumpBox::new_in(right, self.bump),
		};

		Ok(Ast::Filter(AstFilter {
			token: rql_token,
			node: BumpBox::new_in(Ast::Infix(infix), self.bump),
			rql: "",
		}))
	}

	fn identifier_ast(&self, name: &str) -> Ast<'bump> {
		Ast::Identifier(UnqualifiedIdentifier::from_fragment(BumpFragment::Internal {
			text: self.bump.alloc_str(name),
		}))
	}

	fn make_variable_token(&self, name_without_dollar: &str, _gql_token: GqlToken<'bump>) -> RqlToken<'bump> {
		let with_dollar = format!("${}", name_without_dollar);
		let text = self.bump.alloc_str(&with_dollar);
		RqlToken {
			kind: RqlTokenKind::Variable,
			fragment: BumpFragment::Internal {
				text,
			},
		}
	}

	fn make_variable(&self, name_without_dollar: &str, gql_token: GqlToken<'bump>) -> AstVariable<'bump> {
		AstVariable {
			token: self.make_variable_token(name_without_dollar, gql_token),
		}
	}

	fn let_empty_list(&self, name_without_dollar: &str, gql_token: GqlToken<'bump>) -> AstLet<'bump> {
		let var_token = self.make_variable_token(name_without_dollar, gql_token);
		let name = UnqualifiedIdentifier::new(var_token);
		let let_token = self.to_rql_token(gql_token);
		let list = Ast::List(AstList {
			token: let_token,
			nodes: Vec::new(),
		});
		AstLet {
			token: let_token,
			name,
			value: LetValue::Expression(BumpBox::new_in(list, self.bump)),
		}
	}

	fn var_dot_col(&self, var: AstVariable<'bump>, col: &str, token: GqlToken<'bump>) -> Ast<'bump> {
		let rql_token = self.to_rql_token(token);
		let left = Ast::Variable(var);
		let right = self.identifier_ast(col);
		Ast::Infix(AstInfix {
			token: rql_token,
			left: BumpBox::new_in(left, self.bump),
			operator: InfixOperator::AccessTable(rql_token),
			right: BumpBox::new_in(right, self.bump),
		})
	}

	fn eq_filter(&self, left: Ast<'bump>, right: Ast<'bump>, token: GqlToken<'bump>) -> AstFilter<'bump> {
		let rql_token = self.to_rql_token(token);
		let infix = AstInfix {
			token: rql_token,
			left: BumpBox::new_in(left, self.bump),
			operator: InfixOperator::Equal(rql_token),
			right: BumpBox::new_in(right, self.bump),
		};
		AstFilter {
			token: rql_token,
			node: BumpBox::new_in(Ast::Infix(infix), self.bump),
			rql: "",
		}
	}

	fn in_filter(&self, left: Ast<'bump>, right: Ast<'bump>, token: GqlToken<'bump>) -> AstFilter<'bump> {
		let rql_token = self.to_rql_token(token);
		let infix = AstInfix {
			token: rql_token,
			left: BumpBox::new_in(left, self.bump),
			operator: InfixOperator::In(rql_token),
			right: BumpBox::new_in(right, self.bump),
		};
		AstFilter {
			token: rql_token,
			node: BumpBox::new_in(Ast::Infix(infix), self.bump),
			rql: "",
		}
	}

	fn subquery_var_pipe_map(&self, var: AstVariable<'bump>, col: &str, token: GqlToken<'bump>) -> Ast<'bump> {
		let rql_token = self.to_rql_token(token);
		let from_var = Ast::From(AstFrom::Variable {
			token: rql_token,
			variable: var,
		});
		let map_node = Ast::Map(AstMap {
			token: rql_token,
			nodes: vec![self.identifier_ast(col)],
			rql: "",
		});
		let statement = AstStatement {
			nodes: vec![from_var, map_node],
			has_pipes: true,
			is_output: false,
			rql: "",
		};
		Ast::SubQuery(AstSubQuery {
			token: rql_token,
			statement,
		})
	}

	fn boolean_rql_token(&self, gql_token: GqlToken<'bump>) -> RqlToken<'bump> {
		let kind = if gql_token.value() == "true" {
			RqlTokenKind::Literal(RqlLiteral::True)
		} else {
			RqlTokenKind::Literal(RqlLiteral::False)
		};
		RqlToken {
			kind,
			fragment: gql_token.fragment,
		}
	}

	fn to_rql_token(&self, gql_token: GqlToken<'bump>) -> RqlToken<'bump> {
		let kind = match gql_token.kind {
			GqlTokenKind::Name => RqlTokenKind::Identifier,
			GqlTokenKind::IntLiteral => RqlTokenKind::Literal(RqlLiteral::Number),
			GqlTokenKind::FloatLiteral => RqlTokenKind::Literal(RqlLiteral::Number),
			GqlTokenKind::StringLiteral => RqlTokenKind::Literal(RqlLiteral::Text),
			GqlTokenKind::BooleanLiteral => {
				if gql_token.value() == "true" {
					RqlTokenKind::Literal(RqlLiteral::True)
				} else {
					RqlTokenKind::Literal(RqlLiteral::False)
				}
			}
			_ => RqlTokenKind::Identifier,
		};
		RqlToken {
			kind,
			fragment: gql_token.fragment,
		}
	}
}

struct LevelNode<'a, 'bump> {
	index: usize,
	field: &'a AstField<'bump>,
	table_name: String,
	relation: Option<ResolvedRelation>,
	leaves: Vec<&'a AstField<'bump>>,
	children: Vec<LevelNode<'a, 'bump>>,
}

struct ResolvedRelation {
	cardinality: RelationshipCardinality,
	source_column_name: String,
	target_column_name: String,
	junction: Option<ResolvedJunction>,
}

struct ResolvedJunction {
	table_name: String,
	source_column_name: String,
	target_column_name: String,
}

fn column_name_or_err(table: &Table, col_id: ColumnId) -> Result<String, CompilerError> {
	table.columns
		.iter()
		.find(|c| c.id == col_id)
		.map(|c: &Column| c.name.clone())
		.ok_or(CompilerError::ColumnNotFound(col_id, table.name.clone()))
}
