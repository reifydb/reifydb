// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! AST explanation/pretty-printing for debugging and testing.

use std::fmt::Write;

use bumpalo::Bump;

use super::{Expr, Pipeline, Program, Statement};
use crate::{
	ast::{
		expr::{
			literal::Literal,
			query::{FromExpr, JoinExpr, JoinSource, SortDirection, UsingClause},
			special::ForIterable,
		},
		parse::{
			Parser,
			error::{ParseError, ParseErrorKind},
		},
		stmt::{binding::LetValue, dml::InsertSource},
	},
	token::{span::Span, tokenize},
};

/// Explain parsing by showing the AST structure.
///
/// # Arguments
///
/// * `source` - The RQL source code to parse
///
/// # Returns
///
/// A formatted string showing the AST structure as an ASCII tree,
/// or a `ParseError` if parsing fails.
pub fn explain_ast(source: &str) -> Result<String, ParseError> {
	let bump = Bump::new();

	// First tokenize
	let tokens = tokenize(source, &bump).map_err(|e| ParseError {
		kind: ParseErrorKind::Custom(format!("Lex error: {}", e)),
		span: Span::default(),
	})?;

	// Then parse
	let parser = Parser::new(&bump, tokens.tokens.into_bump_slice(), source);
	let result = parser.parse();

	// Check for errors
	if !result.errors.is_empty() {
		return Err(result.errors[0].clone());
	}

	let mut output = String::new();
	let mut formatter = AstFormatter::new(&mut output);
	formatter.format_program(&result.program);

	Ok(output)
}

/// Formatter for AST nodes using ASCII tree drawing.
struct AstFormatter<'a> {
	output: &'a mut String,
	/// Stack tracking whether each ancestor level has more siblings following.
	/// `true` means the ancestor has more siblings (use `|`), `false` means it's the last (use space).
	prefixes: Vec<bool>,
}

impl<'a> AstFormatter<'a> {
	fn new(output: &'a mut String) -> Self {
		Self {
			output,
			prefixes: Vec::new(),
		}
	}

	/// Write the tree prefix for the current depth.
	fn write_prefix(&mut self) {
		for &has_more in &self.prefixes {
			if has_more {
				self.output.push_str("|   ");
			} else {
				self.output.push_str("    ");
			}
		}
	}

	/// Write a branch line with the given text.
	fn write_branch(&mut self, is_last: bool, text: &str) {
		self.write_prefix();
		if is_last {
			self.output.push_str("`-- ");
		} else {
			self.output.push_str("+-- ");
		}
		writeln!(self.output, "{}", text).unwrap();
	}

	/// Write an indexed branch line: `[N] text`
	fn write_indexed(&mut self, is_last: bool, index: usize, text: &str) {
		self.write_prefix();
		if is_last {
			self.output.push_str("`-- ");
		} else {
			self.output.push_str("+-- ");
		}
		writeln!(self.output, "[{}] {}", index, text).unwrap();
	}

	/// Execute a closure with an additional prefix level.
	fn with_child<F>(&mut self, is_last: bool, f: F)
	where
		F: FnOnce(&mut Self),
	{
		self.prefixes.push(!is_last);
		f(self);
		self.prefixes.pop();
	}

	fn format_program(&mut self, program: &Program) {
		writeln!(self.output, "Program").unwrap();
		let len = program.statements.len();
		for (i, stmt) in program.statements.iter().enumerate() {
			let is_last = i == len - 1;
			self.format_statement_indexed(is_last, i, stmt);
		}
	}

	fn format_statement_indexed(&mut self, is_last: bool, index: usize, stmt: &Statement) {
		match stmt {
			Statement::Pipeline(p) => {
				self.write_indexed(is_last, index, "Pipeline");
				self.with_child(is_last, |f| f.format_pipeline(p));
			}
			Statement::Let(l) => {
				self.write_indexed(is_last, index, &format!("Let(${})", l.name));
				self.with_child(is_last, |f| f.format_let_value(&l.value));
			}
			Statement::Assign(a) => {
				self.write_indexed(is_last, index, &format!("Assign(${})", a.name));
				self.with_child(is_last, |f| f.format_expr(true, a.value));
			}
			Statement::Def(d) => {
				let params: Vec<_> = d.parameters.iter().map(|p| p.name).collect();
				self.write_indexed(is_last, index, &format!("Def({}({}))", d.name, params.join(", ")));
				self.with_child(is_last, |f| {
					let len = d.body.len();
					for (i, stmt) in d.body.iter().enumerate() {
						f.format_statement_indexed(i == len - 1, i, stmt);
					}
				});
			}
			Statement::If(i) => {
				self.write_indexed(is_last, index, "If");
				self.with_child(is_last, |f| {
					let has_else_ifs = !i.else_ifs.is_empty();
					let has_else = i.else_branch.is_some();

					// condition
					f.write_branch(false, "condition:");
					f.with_child(false, |f| f.format_expr(true, i.condition));

					// then
					let then_is_last = !has_else_ifs && !has_else;
					f.write_branch(then_is_last, "then:");
					f.with_child(then_is_last, |f| {
						let len = i.then_branch.len();
						for (idx, stmt) in i.then_branch.iter().enumerate() {
							f.format_statement_indexed(idx == len - 1, idx, stmt);
						}
					});

					// else_ifs
					for (idx, else_if) in i.else_ifs.iter().enumerate() {
						let ei_is_last = idx == i.else_ifs.len() - 1 && !has_else;
						f.write_branch(ei_is_last, &format!("else_if[{}]:", idx));
						f.with_child(ei_is_last, |f| {
							f.write_branch(false, "condition:");
							f.with_child(false, |f| f.format_expr(true, else_if.condition));
							f.write_branch(true, "body:");
							f.with_child(true, |f| {
								let len = else_if.body.len();
								for (i, stmt) in else_if.body.iter().enumerate() {
									f.format_statement_indexed(
										i == len - 1,
										i,
										stmt,
									);
								}
							});
						});
					}

					// else
					if let Some(else_branch) = i.else_branch {
						f.write_branch(true, "else:");
						f.with_child(true, |f| {
							let len = else_branch.len();
							for (i, stmt) in else_branch.iter().enumerate() {
								f.format_statement_indexed(i == len - 1, i, stmt);
							}
						});
					}
				});
			}
			Statement::Loop(l) => {
				self.write_indexed(is_last, index, "Loop");
				self.with_child(is_last, |f| {
					let len = l.body.len();
					for (i, stmt) in l.body.iter().enumerate() {
						f.format_statement_indexed(i == len - 1, i, stmt);
					}
				});
			}
			Statement::For(fr) => {
				self.write_indexed(is_last, index, &format!("For(${})", fr.variable));
				self.with_child(is_last, |f| {
					f.write_branch(false, "iterable:");
					f.with_child(false, |f| match &fr.iterable {
						ForIterable::Expr(expr) => {
							f.format_expr(true, expr);
						}
						ForIterable::Pipeline(stages) => {
							let len = stages.len();
							for (i, stage) in stages.iter().enumerate() {
								f.format_expr_indexed(i == len - 1, i, stage);
							}
						}
					});
					f.write_branch(true, "body:");
					f.with_child(true, |f| {
						let len = fr.body.len();
						for (i, stmt) in fr.body.iter().enumerate() {
							f.format_statement_indexed(i == len - 1, i, stmt);
						}
					});
				});
			}
			Statement::Break(_) => {
				self.write_indexed(is_last, index, "Break");
			}
			Statement::Continue(_) => {
				self.write_indexed(is_last, index, "Continue");
			}
			Statement::Return(r) => {
				self.write_indexed(is_last, index, "Return");
				if let Some(value) = r.value {
					self.with_child(is_last, |f| f.format_expr(true, value));
				}
			}
			Statement::Create(c) => {
				self.write_indexed(is_last, index, &format!("Create({:?})", c));
			}
			Statement::Alter(a) => {
				self.write_indexed(is_last, index, &format!("Alter({:?})", a));
			}
			Statement::Drop(d) => {
				self.write_indexed(is_last, index, &format!("Drop({:?})", d));
			}
			Statement::Insert(ins) => {
				let target = match ins.namespace {
					Some(ns) => format!("{}.{}", ns, ins.table),
					None => ins.table.to_string(),
				};
				self.write_indexed(is_last, index, &format!("Insert({})", target));
				self.with_child(is_last, |f| {
					f.write_branch(true, "source:");
					f.with_child(true, |f| match &ins.source {
						InsertSource::Values(rows) => {
							f.write_branch(true, "Values:");
							f.with_child(true, |f| {
								let len = rows.len();
								for (i, row) in rows.iter().enumerate() {
									let row_is_last = i == len - 1;
									f.write_indexed(row_is_last, i, "Row:");
									f.with_child(row_is_last, |f| {
										let rlen = row.len();
										for (j, val) in row.iter().enumerate() {
											f.format_expr_indexed(
												j == rlen - 1,
												j,
												val,
											);
										}
									});
								}
							});
						}
						InsertSource::Query(pipeline) => {
							f.write_branch(true, "Query:");
							f.with_child(true, |f| {
								let len = pipeline.len();
								for (i, stage) in pipeline.iter().enumerate() {
									f.format_expr_indexed(i == len - 1, i, stage);
								}
							});
						}
					});
				});
			}
			Statement::Update(u) => {
				let target = match u.namespace {
					Some(ns) => format!("{}.{}", ns, u.table),
					None => u.table.to_string(),
				};
				self.write_indexed(is_last, index, &format!("Update({})", target));
				self.with_child(is_last, |f| {
					let has_filter = u.filter.is_some();
					f.write_branch(!has_filter, "assignments:");
					f.with_child(!has_filter, |f| {
						let len = u.assignments.len();
						for (i, assign) in u.assignments.iter().enumerate() {
							let a_is_last = i == len - 1;
							f.write_indexed(a_is_last, i, &format!("{} :=", assign.column));
							f.with_child(a_is_last, |f| f.format_expr(true, assign.value));
						}
					});
					if let Some(filter) = u.filter {
						f.write_branch(true, "filter:");
						f.with_child(true, |f| f.format_expr(true, filter));
					}
				});
			}
			Statement::Delete(d) => {
				let target = match d.namespace {
					Some(ns) => format!("{}.{}", ns, d.table),
					None => d.table.to_string(),
				};
				self.write_indexed(is_last, index, &format!("Delete({})", target));
				if let Some(filter) = d.filter {
					self.with_child(is_last, |f| {
						f.write_branch(true, "filter:");
						f.with_child(true, |f| f.format_expr(true, filter));
					});
				}
			}
			Statement::Describe(d) => {
				self.write_indexed(is_last, index, "Describe");
				self.with_child(is_last, |f| f.format_expr(true, d.target));
			}
			Statement::Expression(e) => {
				self.write_indexed(is_last, index, "Expression");
				self.with_child(is_last, |f| f.format_expr(true, e.expr));
			}
		}
	}

	fn format_pipeline(&mut self, pipeline: &Pipeline) {
		let len = pipeline.stages.len();
		for (i, stage) in pipeline.stages.iter().enumerate() {
			self.format_expr_indexed(i == len - 1, i, stage);
		}
	}

	fn format_let_value(&mut self, value: &LetValue) {
		match value {
			LetValue::Expr(e) => self.format_expr(true, e),
			LetValue::Pipeline(stages) => {
				self.write_branch(true, "Pipeline:");
				self.with_child(true, |f| {
					let len = stages.len();
					for (i, stage) in stages.iter().enumerate() {
						f.format_expr_indexed(i == len - 1, i, stage);
					}
				});
			}
		}
	}

	fn format_expr_indexed(&mut self, is_last: bool, index: usize, expr: &Expr) {
		let label = self.expr_label(expr);
		self.write_indexed(is_last, index, &label);
		self.with_child(is_last, |f| f.format_expr_children(expr));
	}

	fn format_expr(&mut self, is_last: bool, expr: &Expr) {
		let label = self.expr_label(expr);
		self.write_branch(is_last, &label);
		self.with_child(is_last, |f| f.format_expr_children(expr));
	}

	fn expr_label(&self, expr: &Expr) -> String {
		match expr {
			Expr::Literal(lit) => match lit {
				Literal::Integer {
					value,
					..
				} => format!("Integer({})", value),
				Literal::Float {
					value,
					..
				} => format!("Float({})", value),
				Literal::String {
					value,
					..
				} => format!("String(\"{}\")", value),
				Literal::Bool {
					value,
					..
				} => format!("Bool({})", value),
				Literal::Undefined {
					..
				} => "Undefined".to_string(),
				Literal::Temporal {
					value,
					..
				} => format!("Temporal({})", value),
			},
			Expr::Identifier(id) => format!("Identifier({})", id.name),
			Expr::QualifiedIdent(q) => {
				let parts: Vec<_> = q.parts.iter().copied().collect();
				format!("QualifiedIdent({})", parts.join("."))
			}
			Expr::Variable(v) => format!("Variable(${})", v.name),
			Expr::Wildcard(_) => "Wildcard".to_string(),
			Expr::Rownum(_) => "Rownum".to_string(),
			Expr::Environment(_) => "Environment($env)".to_string(),
			Expr::Binary(b) => format!("Binary({:?})", b.op),
			Expr::Unary(u) => format!("Unary({:?})", u.op),
			Expr::From(f) => match f {
				FromExpr::Source(s) => {
					if let Some(ns) = s.namespace {
						format!("From({}.{})", ns, s.name)
					} else {
						format!("From({})", s.name)
					}
				}
				FromExpr::Variable(v) => format!("From(${})", v.variable.name),
				FromExpr::Inline(_) => "From(Inline)".to_string(),
				FromExpr::Generator(g) => format!("From(Generator({}))", g.name),
				FromExpr::Environment(_) => "From(Environment)".to_string(),
			},
			Expr::Filter(_) => "Filter".to_string(),
			Expr::Map(_) => "Map".to_string(),
			Expr::Extend(_) => "Extend".to_string(),
			Expr::Sort(_) => "Sort".to_string(),
			Expr::Distinct(_) => "Distinct".to_string(),
			Expr::Take(_) => "Take".to_string(),
			Expr::Join(j) => match j {
				JoinExpr::Inner(_) => "Join(Inner)".to_string(),
				JoinExpr::Left(_) => "Join(Left)".to_string(),
				JoinExpr::Natural(_) => "Join(Natural)".to_string(),
			},
			Expr::Merge(_) => "Merge".to_string(),
			Expr::Window(_) => "Window".to_string(),
			Expr::Aggregate(_) => "Aggregate".to_string(),
			Expr::List(_) => "List".to_string(),
			Expr::Tuple(_) => "Tuple".to_string(),
			Expr::Inline(_) => "Inline".to_string(),
			Expr::Call(c) => {
				if let Expr::Identifier(id) = c.function {
					format!("Call({})", id.name)
				} else {
					"Call".to_string()
				}
			}
			Expr::Apply(a) => format!("Apply({})", a.operator),
			Expr::Between(_) => "Between".to_string(),
			Expr::In(i) => {
				if i.negated {
					"NotIn".to_string()
				} else {
					"In".to_string()
				}
			}
			Expr::Cast(_) => "Cast".to_string(),
			Expr::SubQuery(_) => "SubQuery".to_string(),
			Expr::Exists(e) => {
				if e.negated {
					"NotExists".to_string()
				} else {
					"Exists".to_string()
				}
			}
			Expr::IfExpr(_) => "If".to_string(),
			Expr::LoopExpr(_) => "Loop".to_string(),
			Expr::ForExpr(f) => format!("For(${})", f.variable),
			Expr::Paren(_) => "Paren".to_string(),
		}
	}

	fn format_expr_children(&mut self, expr: &Expr) {
		match expr {
			Expr::Literal(_)
			| Expr::Identifier(_)
			| Expr::QualifiedIdent(_)
			| Expr::Variable(_)
			| Expr::Wildcard(_)
			| Expr::Rownum(_)
			| Expr::Environment(_) => {
				// No children
			}
			Expr::Binary(b) => {
				self.write_branch(false, &format!("left: {}", self.expr_label(b.left)));
				self.with_child(false, |f| f.format_expr_children(b.left));
				self.write_branch(true, &format!("right: {}", self.expr_label(b.right)));
				self.with_child(true, |f| f.format_expr_children(b.right));
			}
			Expr::Unary(u) => {
				self.format_expr(true, u.operand);
			}
			Expr::From(f) => match f {
				FromExpr::Inline(i) => {
					let len = i.rows.len();
					for (idx, row) in i.rows.iter().enumerate() {
						self.format_expr_indexed(idx == len - 1, idx, row);
					}
				}
				FromExpr::Generator(g) => {
					let len = g.params.len();
					for (idx, param) in g.params.iter().enumerate() {
						self.format_expr_indexed(idx == len - 1, idx, param);
					}
				}
				_ => {}
			},
			Expr::Filter(f) => {
				self.format_expr(true, f.predicate);
			}
			Expr::Map(m) => {
				let len = m.projections.len();
				for (i, proj) in m.projections.iter().enumerate() {
					self.format_expr_indexed(i == len - 1, i, proj);
				}
			}
			Expr::Extend(e) => {
				let len = e.extensions.len();
				for (i, ext) in e.extensions.iter().enumerate() {
					self.format_expr_indexed(i == len - 1, i, ext);
				}
			}
			Expr::Sort(s) => {
				let len = s.columns.len();
				for (i, col) in s.columns.iter().enumerate() {
					let dir = match col.direction {
						Some(SortDirection::Asc) => " ASC",
						Some(SortDirection::Desc) => " DESC",
						None => "",
					};
					let col_is_last = i == len - 1;
					self.write_indexed(col_is_last, i, &format!("SortColumn{}", dir));
					self.with_child(col_is_last, |f| f.format_expr(true, col.expr));
				}
			}
			Expr::Distinct(d) => {
				let len = d.columns.len();
				for (i, col) in d.columns.iter().enumerate() {
					self.format_expr_indexed(i == len - 1, i, col);
				}
			}
			Expr::Take(t) => {
				self.format_expr(true, t.count);
			}
			Expr::Join(j) => match j {
				JoinExpr::Inner(inner) => {
					let has_using = !inner.using_clause.pairs.is_empty();
					self.format_join_source(false, &inner.source);
					self.write_branch(!has_using, &format!("alias: {}", inner.alias));
					if has_using {
						self.format_using_clause(true, &inner.using_clause);
					}
				}
				JoinExpr::Left(left) => {
					let has_using = !left.using_clause.pairs.is_empty();
					self.format_join_source(false, &left.source);
					self.write_branch(!has_using, &format!("alias: {}", left.alias));
					if has_using {
						self.format_using_clause(true, &left.using_clause);
					}
				}
				JoinExpr::Natural(nat) => {
					self.format_join_source(false, &nat.source);
					self.write_branch(true, &format!("alias: {}", nat.alias));
				}
			},
			Expr::Merge(m) => {
				self.write_branch(true, "subquery:");
				self.with_child(true, |f| f.format_expr(true, m.subquery));
			}
			Expr::Window(w) => {
				let has_config = !w.config.is_empty();
				let has_aggs = !w.aggregations.is_empty();
				let has_group = !w.group_by.is_empty();

				if has_config {
					let config_is_last = !has_aggs && !has_group;
					self.write_branch(config_is_last, "config:");
					self.with_child(config_is_last, |f| {
						let len = w.config.len();
						for (i, cfg) in w.config.iter().enumerate() {
							let cfg_is_last = i == len - 1;
							f.write_branch(cfg_is_last, &format!("{}:", cfg.key));
							f.with_child(cfg_is_last, |f| f.format_expr(true, cfg.value));
						}
					});
				}
				if has_aggs {
					let aggs_is_last = !has_group;
					self.write_branch(aggs_is_last, "aggregations:");
					self.with_child(aggs_is_last, |f| {
						let len = w.aggregations.len();
						for (i, agg) in w.aggregations.iter().enumerate() {
							f.format_expr_indexed(i == len - 1, i, agg);
						}
					});
				}
				if has_group {
					self.write_branch(true, "group_by:");
					self.with_child(true, |f| {
						let len = w.group_by.len();
						for (i, g) in w.group_by.iter().enumerate() {
							f.format_expr_indexed(i == len - 1, i, g);
						}
					});
				}
			}
			Expr::Aggregate(a) => {
				let has_group = !a.group_by.is_empty();
				let has_aggs = !a.aggregations.is_empty();

				if has_group {
					self.write_branch(!has_aggs, "group_by:");
					self.with_child(!has_aggs, |f| {
						let len = a.group_by.len();
						for (i, g) in a.group_by.iter().enumerate() {
							f.format_expr_indexed(i == len - 1, i, g);
						}
					});
				}
				if has_aggs {
					self.write_branch(true, "aggregations:");
					self.with_child(true, |f| {
						let len = a.aggregations.len();
						for (i, agg) in a.aggregations.iter().enumerate() {
							f.format_expr_indexed(i == len - 1, i, agg);
						}
					});
				}
			}
			Expr::List(l) => {
				let len = l.elements.len();
				for (i, elem) in l.elements.iter().enumerate() {
					self.format_expr_indexed(i == len - 1, i, elem);
				}
			}
			Expr::Tuple(t) => {
				let len = t.elements.len();
				for (i, elem) in t.elements.iter().enumerate() {
					self.format_expr_indexed(i == len - 1, i, elem);
				}
			}
			Expr::Inline(obj) => {
				let len = obj.fields.len();
				for (i, field) in obj.fields.iter().enumerate() {
					let f_is_last = i == len - 1;
					self.write_indexed(f_is_last, i, &format!("Field({})", field.key));
					self.with_child(f_is_last, |f| f.format_expr(true, field.value));
				}
			}
			Expr::Call(c) => {
				let has_fn = !matches!(c.function, Expr::Identifier(_));
				let has_args = !c.arguments.is_empty();

				if has_fn {
					self.write_branch(!has_args, "function:");
					self.with_child(!has_args, |f| f.format_expr(true, c.function));
				}
				if has_args {
					self.write_branch(true, "args:");
					self.with_child(true, |f| {
						let len = c.arguments.len();
						for (i, arg) in c.arguments.iter().enumerate() {
							f.format_expr_indexed(i == len - 1, i, arg);
						}
					});
				}
			}
			Expr::Apply(a) => {
				self.write_branch(true, "expressions:");
				self.with_child(true, |f| {
					let len = a.expressions.len();
					for (i, e) in a.expressions.iter().enumerate() {
						f.format_expr_indexed(i == len - 1, i, e);
					}
				});
			}
			Expr::Between(b) => {
				self.write_branch(false, &format!("value: {}", self.expr_label(b.value)));
				self.with_child(false, |f| f.format_expr_children(b.value));
				self.write_branch(false, &format!("lower: {}", self.expr_label(b.lower)));
				self.with_child(false, |f| f.format_expr_children(b.lower));
				self.write_branch(true, &format!("upper: {}", self.expr_label(b.upper)));
				self.with_child(true, |f| f.format_expr_children(b.upper));
			}
			Expr::In(i) => {
				self.write_branch(false, &format!("value: {}", self.expr_label(i.value)));
				self.with_child(false, |f| f.format_expr_children(i.value));
				self.write_branch(true, &format!("list: {}", self.expr_label(i.list)));
				self.with_child(true, |f| f.format_expr_children(i.list));
			}
			Expr::Cast(c) => {
				self.write_branch(false, "expr:");
				self.with_child(false, |f| f.format_expr(true, c.expr));
				self.write_branch(true, "target_type:");
				self.with_child(true, |f| f.format_expr(true, c.target_type));
			}
			Expr::SubQuery(s) => {
				let len = s.pipeline.len();
				for (i, stage) in s.pipeline.iter().enumerate() {
					self.format_expr_indexed(i == len - 1, i, stage);
				}
			}
			Expr::Exists(e) => {
				self.write_branch(true, "subquery:");
				self.with_child(true, |f| f.format_expr(true, e.subquery));
			}
			Expr::IfExpr(i) => {
				let has_else_ifs = !i.else_ifs.is_empty();
				let has_else = i.else_branch.is_some();

				self.write_branch(false, "condition:");
				self.with_child(false, |f| f.format_expr(true, i.condition));

				let then_is_last = !has_else_ifs && !has_else;
				self.write_branch(then_is_last, "then:");
				self.with_child(then_is_last, |f| {
					let len = i.then_branch.len();
					for (idx, stmt) in i.then_branch.iter().enumerate() {
						f.format_statement_indexed(idx == len - 1, idx, stmt);
					}
				});

				for (idx, else_if) in i.else_ifs.iter().enumerate() {
					let ei_is_last = idx == i.else_ifs.len() - 1 && !has_else;
					self.write_branch(ei_is_last, &format!("else_if[{}]:", idx));
					self.with_child(ei_is_last, |f| {
						f.write_branch(false, "condition:");
						f.with_child(false, |f| f.format_expr(true, else_if.condition));
						f.write_branch(true, "body:");
						f.with_child(true, |f| {
							let len = else_if.body.len();
							for (idx, stmt) in else_if.body.iter().enumerate() {
								f.format_statement_indexed(idx == len - 1, idx, stmt);
							}
						});
					});
				}

				if let Some(else_branch) = i.else_branch {
					self.write_branch(true, "else:");
					self.with_child(true, |f| {
						let len = else_branch.len();
						for (idx, stmt) in else_branch.iter().enumerate() {
							f.format_statement_indexed(idx == len - 1, idx, stmt);
						}
					});
				}
			}
			Expr::LoopExpr(l) => {
				self.write_branch(true, "body:");
				self.with_child(true, |f| {
					let len = l.body.len();
					for (idx, stmt) in l.body.iter().enumerate() {
						f.format_statement_indexed(idx == len - 1, idx, stmt);
					}
				});
			}
			Expr::ForExpr(fr) => {
				self.write_branch(false, "iterable:");
				self.with_child(false, |f| match &fr.iterable {
					ForIterable::Expr(expr) => {
						f.format_expr(true, expr);
					}
					ForIterable::Pipeline(stages) => {
						let len = stages.len();
						for (i, stage) in stages.iter().enumerate() {
							f.format_expr_indexed(i == len - 1, i, stage);
						}
					}
				});
				self.write_branch(true, "body:");
				self.with_child(true, |f| {
					let len = fr.body.len();
					for (idx, stmt) in fr.body.iter().enumerate() {
						f.format_statement_indexed(idx == len - 1, idx, stmt);
					}
				});
			}
			Expr::Paren(inner) => {
				self.format_expr(true, inner);
			}
		}
	}

	fn format_join_source(&mut self, is_last: bool, source: &JoinSource) {
		match source {
			JoinSource::SubQuery(subquery) => {
				self.write_branch(is_last, "subquery:");
				self.with_child(is_last, |f| f.format_expr(true, subquery));
			}
			JoinSource::Primitive(prim) => {
				let table = if let Some(ns) = prim.source.namespace {
					format!("{}.{}", ns, prim.source.name)
				} else {
					prim.source.name.to_string()
				};
				self.write_branch(is_last, &format!("primitive: {}", table));
			}
		}
	}

	fn format_using_clause(&mut self, is_last: bool, clause: &UsingClause) {
		self.write_branch(is_last, "using:");
		self.with_child(is_last, |f| {
			let len = clause.pairs.len();
			for (i, pair) in clause.pairs.iter().enumerate() {
				let p_is_last = i == len - 1;
				f.write_indexed(p_is_last, i, "Pair");
				f.with_child(p_is_last, |pf| {
					pf.write_branch(false, &format!("left: {}", pf.expr_label(pair.left)));
					pf.with_child(false, |f| f.format_expr_children(pair.left));
					pf.write_branch(true, &format!("right: {}", pf.expr_label(pair.right)));
					pf.with_child(true, |f| f.format_expr_children(pair.right));
				});
			}
		});
	}
}
