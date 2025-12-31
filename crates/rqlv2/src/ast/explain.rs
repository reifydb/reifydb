// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! AST explanation/pretty-printing for debugging and testing.

use std::fmt::Write;

use bumpalo::Bump;

use super::{Expr, Pipeline, Program, Statement};
use crate::{
	ast::{
		expr::*,
		parse::{ParseError, ParseErrorKind, Parser},
		stmt::*,
	},
	token::{Span, tokenize},
};

/// Explain parsing by showing the AST structure.
///
/// # Arguments
///
/// * `source` - The RQL source code to parse
///
/// # Returns
///
/// A formatted string showing the AST structure with indentation,
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

/// Formatter for AST nodes.
struct AstFormatter<'a> {
	output: &'a mut String,
	indent: usize,
}

impl<'a> AstFormatter<'a> {
	fn new(output: &'a mut String) -> Self {
		Self {
			output,
			indent: 0,
		}
	}

	fn write_indent(&mut self) {
		for _ in 0..self.indent {
			self.output.push_str("  ");
		}
	}

	fn write_line(&mut self, text: &str) {
		self.write_indent();
		writeln!(self.output, "{}", text).unwrap();
	}

	fn write_indexed(&mut self, index: usize, text: &str) {
		self.write_indent();
		writeln!(self.output, "[{:>3}] {}", index, text).unwrap();
	}

	fn indented<F>(&mut self, f: F)
	where
		F: FnOnce(&mut Self),
	{
		self.indent += 1;
		f(self);
		self.indent -= 1;
	}

	fn format_program(&mut self, program: &Program) {
		self.write_line("Program:");
		self.indented(|f| {
			for (i, stmt) in program.statements.iter().enumerate() {
				f.format_statement_indexed(i, stmt);
			}
		});
	}

	fn format_statement_indexed(&mut self, index: usize, stmt: &Statement) {
		match stmt {
			Statement::Pipeline(p) => {
				self.write_indexed(index, "Statement::Pipeline");
				self.indented(|f| f.format_pipeline(p));
			}
			Statement::Let(l) => {
				let mutability = if l.mutable {
					"mut "
				} else {
					""
				};
				self.write_indexed(index, &format!("Statement::Let({}${})", mutability, l.name));
				self.indented(|f| f.format_let_value(&l.value));
			}
			Statement::Assign(a) => {
				self.write_indexed(index, &format!("Statement::Assign(${})", a.name));
				self.indented(|f| f.format_expr(a.value));
			}
			Statement::Def(d) => {
				let params: Vec<_> = d.parameters.iter().map(|p| p.name).collect();
				self.write_indexed(
					index,
					&format!("Statement::Def({}({}))", d.name, params.join(", ")),
				);
				self.indented(|f| {
					for (i, stmt) in d.body.iter().enumerate() {
						f.format_statement_indexed(i, stmt);
					}
				});
			}
			Statement::If(i) => {
				self.write_indexed(index, "Statement::If");
				self.indented(|f| {
					f.write_line("condition:");
					f.indented(|f| f.format_expr(i.condition));
					f.write_line("then:");
					f.indented(|f| {
						for (idx, stmt) in i.then_branch.iter().enumerate() {
							f.format_statement_indexed(idx, stmt);
						}
					});
					for (idx, else_if) in i.else_ifs.iter().enumerate() {
						f.write_line(&format!("else_if[{}]:", idx));
						f.indented(|f| {
							f.write_line("condition:");
							f.indented(|f| f.format_expr(else_if.condition));
							f.write_line("body:");
							f.indented(|f| {
								for (i, stmt) in else_if.body.iter().enumerate() {
									f.format_statement_indexed(i, stmt);
								}
							});
						});
					}
					if let Some(else_branch) = i.else_branch {
						f.write_line("else:");
						f.indented(|f| {
							for (i, stmt) in else_branch.iter().enumerate() {
								f.format_statement_indexed(i, stmt);
							}
						});
					}
				});
			}
			Statement::Loop(l) => {
				self.write_indexed(index, "Statement::Loop");
				self.indented(|f| {
					for (i, stmt) in l.body.iter().enumerate() {
						f.format_statement_indexed(i, stmt);
					}
				});
			}
			Statement::For(fr) => {
				self.write_indexed(index, &format!("Statement::For(${})", fr.variable));
				self.indented(|f| {
					f.write_line("iterable:");
					f.indented(|f| f.format_expr(fr.iterable));
					f.write_line("body:");
					f.indented(|f| {
						for (i, stmt) in fr.body.iter().enumerate() {
							f.format_statement_indexed(i, stmt);
						}
					});
				});
			}
			Statement::Break(_) => {
				self.write_indexed(index, "Statement::Break");
			}
			Statement::Continue(_) => {
				self.write_indexed(index, "Statement::Continue");
			}
			Statement::Return(r) => {
				self.write_indexed(index, "Statement::Return");
				if let Some(value) = r.value {
					self.indented(|f| f.format_expr(value));
				}
			}
			Statement::Create(c) => {
				self.write_indexed(index, &format!("Statement::Create({:?})", c));
			}
			Statement::Alter(a) => {
				self.write_indexed(index, &format!("Statement::Alter({:?})", a));
			}
			Statement::Drop(d) => {
				self.write_indexed(index, &format!("Statement::Drop({:?})", d));
			}
			Statement::Insert(i) => {
				let target = match i.namespace {
					Some(ns) => format!("{}.{}", ns, i.table),
					None => i.table.to_string(),
				};
				self.write_indexed(index, &format!("Statement::Insert({})", target));
				self.indented(|f| {
					f.write_line("source:");
					match &i.source {
						InsertSource::Values(rows) => {
							f.indented(|f| {
								f.write_line("Values:");
								f.indented(|f| {
									for (i, row) in rows.iter().enumerate() {
										f.write_indexed(i, "Row:");
										f.indented(|f| {
											for (j, val) in
												row.iter().enumerate()
											{
												f.format_expr_indexed(
													j, val,
												);
											}
										});
									}
								});
							});
						}
						InsertSource::Query(pipeline) => {
							f.indented(|f| {
								f.write_line("Query:");
								f.indented(|f| {
									for (i, stage) in pipeline.iter().enumerate() {
										f.format_expr_indexed(i, stage);
									}
								});
							});
						}
					}
				});
			}
			Statement::Update(u) => {
				let target = match u.namespace {
					Some(ns) => format!("{}.{}", ns, u.table),
					None => u.table.to_string(),
				};
				self.write_indexed(index, &format!("Statement::Update({})", target));
				self.indented(|f| {
					f.write_line("assignments:");
					for (i, assign) in u.assignments.iter().enumerate() {
						f.write_indexed(i, &format!("{} := ...", assign.column));
						f.indented(|f| f.format_expr(assign.value));
					}
					if let Some(filter) = u.filter {
						f.write_line("filter:");
						f.indented(|f| f.format_expr(filter));
					}
				});
			}
			Statement::Delete(d) => {
				let target = match d.namespace {
					Some(ns) => format!("{}.{}", ns, d.table),
					None => d.table.to_string(),
				};
				self.write_indexed(index, &format!("Statement::Delete({})", target));
				if let Some(filter) = d.filter {
					self.indented(|f| {
						f.write_line("filter:");
						f.indented(|f| f.format_expr(filter));
					});
				}
			}
			Statement::Describe(d) => {
				self.write_indexed(index, "Statement::Describe");
				self.indented(|f| f.format_expr(d.target));
			}
			Statement::Expression(e) => {
				self.write_indexed(index, "Statement::Expression");
				self.indented(|f| f.format_expr(e.expr));
			}
		}
	}

	fn format_pipeline(&mut self, pipeline: &Pipeline) {
		for (i, stage) in pipeline.stages.iter().enumerate() {
			self.format_expr_indexed(i, stage);
		}
	}

	fn format_let_value(&mut self, value: &LetValue) {
		match value {
			LetValue::Expr(e) => self.format_expr(e),
			LetValue::Pipeline(stages) => {
				self.write_line("Pipeline:");
				self.indented(|f| {
					for (i, stage) in stages.iter().enumerate() {
						f.format_expr_indexed(i, stage);
					}
				});
			}
		}
	}

	fn format_expr_indexed(&mut self, index: usize, expr: &Expr) {
		let label = self.expr_label(expr);
		self.write_indexed(index, &label);
		self.indented(|f| f.format_expr_children(expr));
	}

	fn format_expr(&mut self, expr: &Expr) {
		let label = self.expr_label(expr);
		self.write_line(&label);
		self.indented(|f| f.format_expr_children(expr));
	}

	fn expr_label(&self, expr: &Expr) -> String {
		match expr {
			Expr::Literal(lit) => match lit {
				Literal::Integer {
					value,
					..
				} => format!("Expr::Literal(Integer({}))", value),
				Literal::Float {
					value,
					..
				} => format!("Expr::Literal(Float({}))", value),
				Literal::String {
					value,
					..
				} => format!("Expr::Literal(String(\"{}\"))", value),
				Literal::Bool {
					value,
					..
				} => format!("Expr::Literal(Bool({}))", value),
				Literal::Undefined {
					..
				} => "Expr::Literal(Undefined)".to_string(),
				Literal::Temporal {
					value,
					..
				} => format!("Expr::Literal(Temporal({}))", value),
			},
			Expr::Identifier(id) => format!("Expr::Identifier(\"{}\")", id.name),
			Expr::QualifiedIdent(q) => {
				let parts: Vec<_> = q.parts.iter().copied().collect();
				format!("Expr::QualifiedIdent(\"{}\")", parts.join("."))
			}
			Expr::Variable(v) => format!("Expr::Variable(${})", v.name),
			Expr::Wildcard(_) => "Expr::Wildcard".to_string(),
			Expr::Rownum(_) => "Expr::Rownum".to_string(),
			Expr::Environment(_) => "Expr::Environment($env)".to_string(),
			Expr::Binary(b) => format!("Expr::Binary({:?})", b.op),
			Expr::Unary(u) => format!("Expr::Unary({:?})", u.op),
			Expr::From(f) => match f {
				FromExpr::Source(s) => {
					if let Some(ns) = s.namespace {
						format!("Expr::From(Source(\"{}.{}\"))", ns, s.name)
					} else {
						format!("Expr::From(Source(\"{}\"))", s.name)
					}
				}
				FromExpr::Variable(v) => format!("Expr::From(Variable(${})", v.variable.name),
				FromExpr::Inline(_) => "Expr::From(Inline)".to_string(),
				FromExpr::Generator(g) => format!("Expr::From(Generator({}))", g.name),
				FromExpr::Environment(_) => "Expr::From(Environment)".to_string(),
			},
			Expr::Filter(_) => "Expr::Filter".to_string(),
			Expr::Map(_) => "Expr::Map".to_string(),
			Expr::Extend(_) => "Expr::Extend".to_string(),
			Expr::Sort(_) => "Expr::Sort".to_string(),
			Expr::Distinct(_) => "Expr::Distinct".to_string(),
			Expr::Take(_) => "Expr::Take".to_string(),
			Expr::Join(j) => match j {
				JoinExpr::Inner(_) => "Expr::Join(Inner)".to_string(),
				JoinExpr::Left(_) => "Expr::Join(Left)".to_string(),
				JoinExpr::Natural(_) => "Expr::Join(Natural)".to_string(),
			},
			Expr::Merge(_) => "Expr::Merge".to_string(),
			Expr::Window(_) => "Expr::Window".to_string(),
			Expr::Aggregate(_) => "Expr::Aggregate".to_string(),
			Expr::List(_) => "Expr::List".to_string(),
			Expr::Tuple(_) => "Expr::Tuple".to_string(),
			Expr::Inline(_) => "Expr::Inline".to_string(),
			Expr::Call(c) => {
				if let Expr::Identifier(id) = c.function {
					format!("Expr::Call({})", id.name)
				} else {
					"Expr::Call".to_string()
				}
			}
			Expr::Apply(a) => format!("Expr::Apply({})", a.operator),
			Expr::Between(_) => "Expr::Between".to_string(),
			Expr::In(i) => {
				if i.negated {
					"Expr::NotIn".to_string()
				} else {
					"Expr::In".to_string()
				}
			}
			Expr::Cast(_) => "Expr::Cast".to_string(),
			Expr::SubQuery(_) => "Expr::SubQuery".to_string(),
			Expr::IfExpr(_) => "Expr::If".to_string(),
			Expr::Paren(_) => "Expr::Paren".to_string(),
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
				self.write_line("left:");
				self.indented(|f| f.format_expr(b.left));
				self.write_line("right:");
				self.indented(|f| f.format_expr(b.right));
			}
			Expr::Unary(u) => {
				self.format_expr(u.operand);
			}
			Expr::From(f) => match f {
				FromExpr::Inline(i) => {
					for (idx, row) in i.rows.iter().enumerate() {
						self.format_expr_indexed(idx, row);
					}
				}
				FromExpr::Generator(g) => {
					for (idx, param) in g.params.iter().enumerate() {
						self.format_expr_indexed(idx, param);
					}
				}
				_ => {}
			},
			Expr::Filter(f) => {
				self.format_expr(f.predicate);
			}
			Expr::Map(m) => {
				for (i, proj) in m.projections.iter().enumerate() {
					self.format_expr_indexed(i, proj);
				}
			}
			Expr::Extend(e) => {
				for (i, ext) in e.extensions.iter().enumerate() {
					self.format_expr_indexed(i, ext);
				}
			}
			Expr::Sort(s) => {
				for (i, col) in s.columns.iter().enumerate() {
					let dir = match col.direction {
						Some(SortDirection::Asc) => " ASC",
						Some(SortDirection::Desc) => " DESC",
						None => "",
					};
					self.write_indexed(i, &format!("SortColumn{}", dir));
					self.indented(|f| f.format_expr(col.expr));
				}
			}
			Expr::Distinct(d) => {
				for (i, col) in d.columns.iter().enumerate() {
					self.format_expr_indexed(i, col);
				}
			}
			Expr::Take(t) => {
				self.format_expr(t.count);
			}
			Expr::Join(j) => match j {
				JoinExpr::Inner(inner) => {
					self.write_line("subquery:");
					self.indented(|f| f.format_expr(inner.subquery));
					self.write_line(&format!("alias: {}", inner.alias));
					self.format_using_clause(&inner.using_clause);
				}
				JoinExpr::Left(left) => {
					self.write_line("subquery:");
					self.indented(|f| f.format_expr(left.subquery));
					self.write_line(&format!("alias: {}", left.alias));
					self.format_using_clause(&left.using_clause);
				}
				JoinExpr::Natural(nat) => {
					self.write_line("subquery:");
					self.indented(|f| f.format_expr(nat.subquery));
					self.write_line(&format!("alias: {}", nat.alias));
				}
			},
			Expr::Merge(m) => {
				self.write_line("subquery:");
				self.indented(|f| f.format_expr(m.subquery));
			}
			Expr::Window(w) => {
				if !w.config.is_empty() {
					self.write_line("config:");
					self.indented(|f| {
						for cfg in w.config.iter() {
							f.write_line(&format!("{}: ...", cfg.key));
							f.indented(|f| f.format_expr(cfg.value));
						}
					});
				}
				if !w.aggregations.is_empty() {
					self.write_line("aggregations:");
					self.indented(|f| {
						for (i, agg) in w.aggregations.iter().enumerate() {
							f.format_expr_indexed(i, agg);
						}
					});
				}
				if !w.group_by.is_empty() {
					self.write_line("group_by:");
					self.indented(|f| {
						for (i, g) in w.group_by.iter().enumerate() {
							f.format_expr_indexed(i, g);
						}
					});
				}
			}
			Expr::Aggregate(a) => {
				if !a.group_by.is_empty() {
					self.write_line("group_by:");
					self.indented(|f| {
						for (i, g) in a.group_by.iter().enumerate() {
							f.format_expr_indexed(i, g);
						}
					});
				}
				if !a.aggregations.is_empty() {
					self.write_line("aggregations:");
					self.indented(|f| {
						for (i, agg) in a.aggregations.iter().enumerate() {
							f.format_expr_indexed(i, agg);
						}
					});
				}
			}
			Expr::List(l) => {
				for (i, elem) in l.elements.iter().enumerate() {
					self.format_expr_indexed(i, elem);
				}
			}
			Expr::Tuple(t) => {
				for (i, elem) in t.elements.iter().enumerate() {
					self.format_expr_indexed(i, elem);
				}
			}
			Expr::Inline(obj) => {
				for (i, field) in obj.fields.iter().enumerate() {
					self.write_indexed(i, &format!("Field(\"{}\")", field.key));
					self.indented(|f| f.format_expr(field.value));
				}
			}
			Expr::Call(c) => {
				if !matches!(c.function, Expr::Identifier(_)) {
					self.write_line("function:");
					self.indented(|f| f.format_expr(c.function));
				}
				if !c.arguments.is_empty() {
					self.write_line("arguments:");
					self.indented(|f| {
						for (i, arg) in c.arguments.iter().enumerate() {
							f.format_expr_indexed(i, arg);
						}
					});
				}
			}
			Expr::Apply(a) => {
				self.write_line("expressions:");
				self.indented(|f| {
					for (i, e) in a.expressions.iter().enumerate() {
						f.format_expr_indexed(i, e);
					}
				});
			}
			Expr::Between(b) => {
				self.write_line("value:");
				self.indented(|f| f.format_expr(b.value));
				self.write_line("lower:");
				self.indented(|f| f.format_expr(b.lower));
				self.write_line("upper:");
				self.indented(|f| f.format_expr(b.upper));
			}
			Expr::In(i) => {
				self.write_line("value:");
				self.indented(|f| f.format_expr(i.value));
				self.write_line("list:");
				self.indented(|f| f.format_expr(i.list));
			}
			Expr::Cast(c) => {
				self.write_line("expr:");
				self.indented(|f| f.format_expr(c.expr));
				self.write_line("target_type:");
				self.indented(|f| f.format_expr(c.target_type));
			}
			Expr::SubQuery(s) => {
				for (i, stage) in s.pipeline.iter().enumerate() {
					self.format_expr_indexed(i, stage);
				}
			}
			Expr::IfExpr(i) => {
				self.write_line("condition:");
				self.indented(|f| f.format_expr(i.condition));
				self.write_line("then:");
				self.indented(|f| f.format_expr(i.then_branch));
				for (idx, else_if) in i.else_ifs.iter().enumerate() {
					self.write_line(&format!("else_if[{}]:", idx));
					self.indented(|f| {
						f.write_line("condition:");
						f.indented(|f| f.format_expr(else_if.condition));
						f.write_line("then:");
						f.indented(|f| f.format_expr(else_if.then_branch));
					});
				}
				if let Some(else_branch) = i.else_branch {
					self.write_line("else:");
					self.indented(|f| f.format_expr(else_branch));
				}
			}
			Expr::Paren(inner) => {
				self.format_expr(inner);
			}
		}
	}

	fn format_using_clause(&mut self, clause: &UsingClause) {
		if !clause.pairs.is_empty() {
			self.write_line("using:");
			self.indented(|f| {
				for (i, pair) in clause.pairs.iter().enumerate() {
					f.write_indexed(i, "Pair:");
					f.indented(|f| {
						f.write_line("left:");
						f.indented(|f| f.format_expr(pair.left));
						f.write_line("right:");
						f.indented(|f| f.format_expr(pair.right));
					});
				}
			});
		}
	}
}
