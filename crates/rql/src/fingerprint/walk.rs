// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! AST walk that serializes structural shape into a canonical byte buffer.
//!
//! Literal *values* are omitted (only the kind tag is written), so two
//! queries differing only in constants produce identical byte sequences.

use crate::ast::{
	ast::*,
	identifier::{MaybeQualifiedColumnIdentifier, MaybeQualifiedFunctionIdentifier, UnresolvedSchemaIdentifier},
};

pub(crate) struct FingerprintBuffer(Vec<u8>);

impl FingerprintBuffer {
	pub fn new() -> Self {
		Self(Vec::with_capacity(256))
	}

	#[inline]
	pub fn write_u8(&mut self, v: u8) {
		self.0.push(v);
	}

	#[inline]
	fn write_u16(&mut self, v: u16) {
		self.0.extend_from_slice(&v.to_le_bytes());
	}

	/// Length-prefixed string so adjacent fields stay unambiguous.
	#[inline]
	fn write_str(&mut self, s: &str) {
		self.write_u16(s.len() as u16);
		self.0.extend_from_slice(s.as_bytes());
	}

	pub fn as_bytes(&self) -> &[u8] {
		&self.0
	}
}

mod tag {
	// Literals (value omitted)
	pub const LIT_NUMBER: u8 = 0x01;
	pub const LIT_TEXT: u8 = 0x02;
	pub const LIT_BOOLEAN: u8 = 0x03;
	pub const LIT_TEMPORAL: u8 = 0x04;
	pub const LIT_NONE: u8 = 0x05;

	// Atoms
	pub const IDENTIFIER: u8 = 0x10;
	pub const VARIABLE: u8 = 0x11;
	pub const WILDCARD: u8 = 0x12;
	pub const ROWNUM: u8 = 0x13;
	pub const ENVIRONMENT: u8 = 0x14;
	pub const NOP: u8 = 0x15;

	// Operators
	pub const INFIX: u8 = 0x20;
	pub const PREFIX: u8 = 0x21;
	pub const BETWEEN: u8 = 0x22;

	// Query pipeline
	pub const FILTER: u8 = 0x30;
	pub const MAP: u8 = 0x31;
	pub const SORT: u8 = 0x32;
	pub const TAKE: u8 = 0x33;
	pub const DISTINCT: u8 = 0x34;
	pub const FROM_SOURCE: u8 = 0x35;
	pub const FROM_VARIABLE: u8 = 0x36;
	pub const FROM_ENVIRONMENT: u8 = 0x37;
	pub const FROM_INLINE: u8 = 0x38;
	pub const FROM_GENERATOR: u8 = 0x39;
	pub const AGGREGATE: u8 = 0x3A;
	pub const GATE: u8 = 0x3B;
	pub const EXTEND: u8 = 0x3C;
	pub const PATCH: u8 = 0x3D;
	pub const WINDOW: u8 = 0x3E;
	pub const APPLY: u8 = 0x3F;

	// Collections
	pub const LIST: u8 = 0x40;
	pub const INLINE: u8 = 0x41;
	pub const TUPLE: u8 = 0x42;
	pub const BLOCK: u8 = 0x43;
	pub const SUBQUERY: u8 = 0x44;

	// DML
	pub const INSERT: u8 = 0x50;
	pub const UPDATE: u8 = 0x51;
	pub const DELETE: u8 = 0x52;
	pub const APPEND_VAR: u8 = 0x53;
	pub const APPEND_QUERY: u8 = 0x54;

	// Joins
	pub const JOIN_INNER: u8 = 0x60;
	pub const JOIN_LEFT: u8 = 0x61;
	pub const JOIN_NATURAL: u8 = 0x62;

	// Control flow
	pub const IF: u8 = 0x70;
	pub const FOR: u8 = 0x71;
	pub const WHILE: u8 = 0x72;
	pub const LOOP: u8 = 0x73;
	pub const LET: u8 = 0x74;
	pub const BREAK: u8 = 0x75;
	pub const CONTINUE: u8 = 0x76;
	pub const RETURN: u8 = 0x77;
	pub const MATCH: u8 = 0x78;

	// Functions / calls
	pub const CALL: u8 = 0x80;
	pub const CALL_FUNCTION: u8 = 0x81;
	pub const CAST: u8 = 0x82;
	pub const CLOSURE: u8 = 0x83;
	pub const DEF_FUNCTION: u8 = 0x84;
	pub const GENERATOR: u8 = 0x85;
	pub const STATEMENT_EXPR: u8 = 0x86;

	// DDL / admin
	pub const CREATE: u8 = 0x90;
	pub const ALTER: u8 = 0x91;
	pub const DROP: u8 = 0x92;
	pub const DESCRIBE: u8 = 0x93;
	pub const GRANT: u8 = 0x94;
	pub const REVOKE: u8 = 0x95;
	pub const IDENTITY: u8 = 0x96;
	pub const REQUIRE: u8 = 0x97;
	pub const MIGRATE: u8 = 0x98;
	pub const ROLLBACK_MIGRATION: u8 = 0x99;
	pub const RUN_TESTS: u8 = 0x9A;
	pub const ASSERT: u8 = 0x9B;
	pub const DISPATCH: u8 = 0x9C;
	pub const SUM_TYPE_CTOR: u8 = 0x9D;
	pub const IS_VARIANT: u8 = 0x9E;

	// Infix operators
	pub const OP_ADD: u8 = 0xA0;
	pub const OP_SUBTRACT: u8 = 0xA1;
	pub const OP_MULTIPLY: u8 = 0xA2;
	pub const OP_DIVIDE: u8 = 0xA3;
	pub const OP_REM: u8 = 0xA4;
	pub const OP_EQUAL: u8 = 0xA5;
	pub const OP_NOT_EQUAL: u8 = 0xA6;
	pub const OP_LESS_THAN: u8 = 0xA7;
	pub const OP_LESS_THAN_EQUAL: u8 = 0xA8;
	pub const OP_GREATER_THAN: u8 = 0xA9;
	pub const OP_GREATER_THAN_EQUAL: u8 = 0xAA;
	pub const OP_AND: u8 = 0xAB;
	pub const OP_OR: u8 = 0xAC;
	pub const OP_XOR: u8 = 0xAD;
	pub const OP_IN: u8 = 0xAE;
	pub const OP_NOT_IN: u8 = 0xAF;
	pub const OP_CONTAINS: u8 = 0xB0;
	pub const OP_AS: u8 = 0xB1;
	pub const OP_ASSIGN: u8 = 0xB2;
	pub const OP_CALL: u8 = 0xB3;
	pub const OP_ACCESS_NAMESPACE: u8 = 0xB4;
	pub const OP_ACCESS_TABLE: u8 = 0xB5;
	pub const OP_TYPE_ASCRIPTION: u8 = 0xB6;

	// Prefix operators
	pub const OP_PREFIX_PLUS: u8 = 0xC0;
	pub const OP_PREFIX_NEGATE: u8 = 0xC1;
	pub const OP_PREFIX_NOT: u8 = 0xC2;

	// Take value kinds
	pub const TAKE_LITERAL: u8 = 0xD0;
	pub const TAKE_VARIABLE: u8 = 0xD1;
}

pub(crate) fn fingerprint_ast(buf: &mut FingerprintBuffer, ast: &Ast<'_>) {
	match ast {
		Ast::Literal(lit) => match lit {
			AstLiteral::Number(_) => buf.write_u8(tag::LIT_NUMBER),
			AstLiteral::Text(_) => buf.write_u8(tag::LIT_TEXT),
			AstLiteral::Boolean(_) => buf.write_u8(tag::LIT_BOOLEAN),
			AstLiteral::Temporal(_) => buf.write_u8(tag::LIT_TEMPORAL),
			AstLiteral::None(_) => buf.write_u8(tag::LIT_NONE),
		},

		Ast::Identifier(ident) => {
			buf.write_u8(tag::IDENTIFIER);
			buf.write_str(ident.text());
		}
		Ast::Variable(var) => {
			buf.write_u8(tag::VARIABLE);
			buf.write_str(var.name());
		}
		Ast::Wildcard(_) => buf.write_u8(tag::WILDCARD),
		Ast::Rownum(_) => buf.write_u8(tag::ROWNUM),
		Ast::Environment(_) => buf.write_u8(tag::ENVIRONMENT),
		Ast::Nop => buf.write_u8(tag::NOP),

		Ast::Infix(node) => {
			buf.write_u8(tag::INFIX);
			write_infix_op(buf, &node.operator);
			fingerprint_ast(buf, &node.left);
			fingerprint_ast(buf, &node.right);
		}
		Ast::Prefix(node) => {
			buf.write_u8(tag::PREFIX);
			match &node.operator {
				AstPrefixOperator::Plus(_) => buf.write_u8(tag::OP_PREFIX_PLUS),
				AstPrefixOperator::Negate(_) => buf.write_u8(tag::OP_PREFIX_NEGATE),
				AstPrefixOperator::Not(_) => buf.write_u8(tag::OP_PREFIX_NOT),
			}
			fingerprint_ast(buf, &node.node);
		}
		Ast::Between(node) => {
			buf.write_u8(tag::BETWEEN);
			fingerprint_ast(buf, &node.value);
			fingerprint_ast(buf, &node.lower);
			fingerprint_ast(buf, &node.upper);
		}

		Ast::Filter(node) => {
			buf.write_u8(tag::FILTER);
			fingerprint_ast(buf, &node.node);
		}
		Ast::Map(node) => {
			buf.write_u8(tag::MAP);
			fingerprint_ast_slice(buf, &node.nodes);
		}
		Ast::Sort(node) => {
			buf.write_u8(tag::SORT);
			for col in &node.columns {
				write_column_id(buf, col);
			}
			for dir in &node.directions {
				match dir {
					Some(frag) => buf.write_str(frag.text()),
					None => buf.write_u8(0),
				}
			}
		}
		Ast::Take(node) => {
			buf.write_u8(tag::TAKE);
			match &node.take {
				AstTakeValue::Literal(_) => buf.write_u8(tag::TAKE_LITERAL),
				AstTakeValue::Variable(t) => {
					buf.write_u8(tag::TAKE_VARIABLE);
					buf.write_str(t.value());
				}
			}
		}
		Ast::Distinct(node) => {
			buf.write_u8(tag::DISTINCT);
			for col in &node.columns {
				write_column_id(buf, col);
			}
		}
		Ast::From(node) => match node {
			AstFrom::Source {
				source,
				..
			} => {
				buf.write_u8(tag::FROM_SOURCE);
				write_unresolved_id(buf, source);
			}
			AstFrom::Variable {
				variable,
				..
			} => {
				buf.write_u8(tag::FROM_VARIABLE);
				buf.write_str(variable.name());
			}
			AstFrom::Environment {
				..
			} => {
				buf.write_u8(tag::FROM_ENVIRONMENT);
			}
			AstFrom::Inline {
				list,
				..
			} => {
				buf.write_u8(tag::FROM_INLINE);
				fingerprint_ast_slice(buf, &list.nodes);
			}
			AstFrom::Generator(generator) => {
				buf.write_u8(tag::FROM_GENERATOR);
				buf.write_str(generator.name.text());
				fingerprint_ast_slice(buf, &generator.nodes);
			}
		},
		Ast::Aggregate(node) => {
			buf.write_u8(tag::AGGREGATE);
			fingerprint_ast_slice(buf, &node.by);
			fingerprint_ast_slice(buf, &node.map);
		}
		Ast::Gate(node) => {
			buf.write_u8(tag::GATE);
			fingerprint_ast(buf, &node.node);
		}
		Ast::Extend(node) => {
			buf.write_u8(tag::EXTEND);
			fingerprint_ast_slice(buf, &node.nodes);
		}
		Ast::Patch(node) => {
			buf.write_u8(tag::PATCH);
			fingerprint_ast_slice(buf, &node.assignments);
		}
		Ast::Window(node) => {
			buf.write_u8(tag::WINDOW);
			buf.write_u8(node.kind as u8);
			for cfg in &node.config {
				buf.write_str(cfg.key.text());
				fingerprint_ast(buf, &cfg.value);
			}
			fingerprint_ast_slice(buf, &node.aggregations);
			fingerprint_ast_slice(buf, &node.group_by);
		}
		Ast::Apply(node) => {
			buf.write_u8(tag::APPLY);
			buf.write_str(node.operator.text());
			fingerprint_ast_slice(buf, &node.expressions);
		}

		Ast::List(node) => {
			buf.write_u8(tag::LIST);
			fingerprint_ast_slice(buf, &node.nodes);
		}
		Ast::Inline(node) => {
			buf.write_u8(tag::INLINE);
			for kv in &node.keyed_values {
				buf.write_str(kv.key.text());
				fingerprint_ast(buf, &kv.value);
			}
		}
		Ast::Tuple(node) => {
			buf.write_u8(tag::TUPLE);
			fingerprint_ast_slice(buf, &node.nodes);
		}
		Ast::Block(node) => {
			buf.write_u8(tag::BLOCK);
			for stmt in &node.statements {
				write_statement(buf, stmt);
			}
		}
		Ast::SubQuery(node) => {
			buf.write_u8(tag::SUBQUERY);
			write_statement(buf, &node.statement);
		}

		Ast::Insert(node) => {
			buf.write_u8(tag::INSERT);
			write_unresolved_id(buf, &node.target);
			fingerprint_ast(buf, &node.source);
			write_optional_returning(buf, &node.returning);
		}
		Ast::Update(node) => {
			buf.write_u8(tag::UPDATE);
			write_unresolved_id(buf, &node.target);
			fingerprint_ast_slice(buf, &node.assignments);
			fingerprint_ast(buf, &node.filter);
			if let Some(take) = &node.take {
				fingerprint_ast(buf, take);
			}
			write_optional_returning(buf, &node.returning);
		}
		Ast::Delete(node) => {
			buf.write_u8(tag::DELETE);
			write_unresolved_id(buf, &node.target);
			fingerprint_ast(buf, &node.filter);
			if let Some(take) = &node.take {
				fingerprint_ast(buf, take);
			}
			write_optional_returning(buf, &node.returning);
		}
		Ast::Append(node) => match node {
			AstAppend::IntoVariable {
				target,
				source,
				..
			} => {
				buf.write_u8(tag::APPEND_VAR);
				buf.write_str(target.name());
				match source {
					AstAppendSource::Statement(stmt) => write_statement(buf, stmt),
					AstAppendSource::Inline(list) => fingerprint_ast_slice(buf, &list.nodes),
				}
			}
			AstAppend::Query {
				with,
				..
			} => {
				buf.write_u8(tag::APPEND_QUERY);
				write_statement(buf, &with.statement);
			}
		},

		Ast::Join(node) => match node {
			AstJoin::InnerJoin {
				with,
				using_clause,
				alias,
				..
			} => {
				buf.write_u8(tag::JOIN_INNER);
				buf.write_str(alias.text());
				write_statement(buf, &with.statement);
				write_using_clause(buf, using_clause);
			}
			AstJoin::LeftJoin {
				with,
				using_clause,
				alias,
				..
			} => {
				buf.write_u8(tag::JOIN_LEFT);
				buf.write_str(alias.text());
				write_statement(buf, &with.statement);
				write_using_clause(buf, using_clause);
			}
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				..
			} => {
				buf.write_u8(tag::JOIN_NATURAL);
				buf.write_u8(join_type.map_or(0, |jt| jt as u8));
				buf.write_str(alias.text());
				write_statement(buf, &with.statement);
			}
		},

		Ast::If(node) => {
			buf.write_u8(tag::IF);
			fingerprint_ast(buf, &node.condition);
			write_block(buf, &node.then_block);
			for else_if in &node.else_ifs {
				fingerprint_ast(buf, &else_if.condition);
				write_block(buf, &else_if.then_block);
			}
			if let Some(else_block) = &node.else_block {
				write_block(buf, else_block);
			}
		}
		Ast::For(node) => {
			buf.write_u8(tag::FOR);
			buf.write_str(node.variable.name());
			fingerprint_ast(buf, &node.iterable);
			write_block(buf, &node.body);
		}
		Ast::While(node) => {
			buf.write_u8(tag::WHILE);
			fingerprint_ast(buf, &node.condition);
			write_block(buf, &node.body);
		}
		Ast::Loop(node) => {
			buf.write_u8(tag::LOOP);
			write_block(buf, &node.body);
		}
		Ast::Let(node) => {
			buf.write_u8(tag::LET);
			buf.write_str(node.name.text());
			match &node.value {
				LetValue::Expression(expr) => fingerprint_ast(buf, expr),
				LetValue::Statement(stmt) => write_statement(buf, stmt),
			}
		}
		Ast::Break(_) => buf.write_u8(tag::BREAK),
		Ast::Continue(_) => buf.write_u8(tag::CONTINUE),
		Ast::Return(node) => {
			buf.write_u8(tag::RETURN);
			if let Some(val) = &node.value {
				fingerprint_ast(buf, val);
			}
		}
		Ast::Match(node) => {
			buf.write_u8(tag::MATCH);
			if let Some(subject) = &node.subject {
				fingerprint_ast(buf, subject);
			}
			for arm in &node.arms {
				write_match_arm(buf, arm);
			}
		}

		Ast::Call(node) => {
			buf.write_u8(tag::CALL);
			write_function_id(buf, &node.function);
			fingerprint_ast_slice(buf, &node.arguments.nodes);
		}
		Ast::CallFunction(node) => {
			buf.write_u8(tag::CALL_FUNCTION);
			write_function_id(buf, &node.function);
			fingerprint_ast_slice(buf, &node.arguments.nodes);
		}
		Ast::Cast(node) => {
			buf.write_u8(tag::CAST);
			fingerprint_ast_slice(buf, &node.tuple.nodes);
		}
		Ast::Closure(node) => {
			buf.write_u8(tag::CLOSURE);
			for param in &node.parameters {
				buf.write_str(param.variable.name());
			}
			write_block(buf, &node.body);
		}
		Ast::DefFunction(node) => {
			buf.write_u8(tag::DEF_FUNCTION);
			buf.write_str(node.name.text());
			for param in &node.parameters {
				buf.write_str(param.variable.name());
			}
			write_block(buf, &node.body);
		}
		Ast::Generator(node) => {
			buf.write_u8(tag::GENERATOR);
			buf.write_str(node.name.text());
			fingerprint_ast_slice(buf, &node.nodes);
		}
		Ast::StatementExpression(node) => {
			buf.write_u8(tag::STATEMENT_EXPR);
			fingerprint_ast(buf, &node.expression);
		}

		Ast::Create(node) => {
			buf.write_u8(tag::CREATE);
			buf.write_str(node.token().value());
		}
		Ast::Alter(node) => {
			buf.write_u8(tag::ALTER);
			buf.write_str(node.token().value());
		}
		Ast::Drop(node) => {
			buf.write_u8(tag::DROP);
			buf.write_str(node.token().value());
		}
		Ast::Describe(AstDescribe::Query {
			node,
			..
		}) => {
			buf.write_u8(tag::DESCRIBE);
			fingerprint_ast(buf, node);
		}
		Ast::Grant(_) => buf.write_u8(tag::GRANT),
		Ast::Revoke(_) => buf.write_u8(tag::REVOKE),
		Ast::Identity(_) => buf.write_u8(tag::IDENTITY),
		Ast::Require(_) => buf.write_u8(tag::REQUIRE),
		Ast::Migrate(_) => buf.write_u8(tag::MIGRATE),
		Ast::RollbackMigration(_) => buf.write_u8(tag::ROLLBACK_MIGRATION),
		Ast::RunTests(_) => buf.write_u8(tag::RUN_TESTS),
		Ast::Assert(node) => {
			buf.write_u8(tag::ASSERT);
			if let Some(n) = &node.node {
				fingerprint_ast(buf, n);
			}
		}
		Ast::Dispatch(node) => {
			buf.write_u8(tag::DISPATCH);
			buf.write_str(node.variant.text());
			for (name, val) in &node.fields {
				buf.write_str(name.text());
				fingerprint_ast(buf, val);
			}
		}
		Ast::SumTypeConstructor(node) => {
			buf.write_u8(tag::SUM_TYPE_CTOR);
			buf.write_str(node.namespace.text());
			buf.write_str(node.sumtype_name.text());
			buf.write_str(node.variant_name.text());
			for kv in &node.columns.keyed_values {
				buf.write_str(kv.key.text());
				fingerprint_ast(buf, &kv.value);
			}
		}
		Ast::IsVariant(node) => {
			buf.write_u8(tag::IS_VARIANT);
			fingerprint_ast(buf, &node.expression);
			if let Some(ns) = &node.namespace {
				buf.write_str(ns.text());
			}
			buf.write_str(node.sumtype_name.text());
			buf.write_str(node.variant_name.text());
		}
	}
}

pub(crate) fn fingerprint_ast_slice(buf: &mut FingerprintBuffer, nodes: &[Ast<'_>]) {
	buf.write_u16(nodes.len() as u16);
	for node in nodes {
		fingerprint_ast(buf, node);
	}
}

fn write_statement(buf: &mut FingerprintBuffer, stmt: &AstStatement<'_>) {
	buf.write_u8(stmt.has_pipes as u8);
	buf.write_u8(stmt.is_output as u8);
	fingerprint_ast_slice(buf, &stmt.nodes);
}

fn write_block(buf: &mut FingerprintBuffer, block: &AstBlock<'_>) {
	buf.write_u16(block.statements.len() as u16);
	for stmt in &block.statements {
		write_statement(buf, stmt);
	}
}

fn write_infix_op(buf: &mut FingerprintBuffer, op: &InfixOperator<'_>) {
	buf.write_u8(match op {
		InfixOperator::Add(_) => tag::OP_ADD,
		InfixOperator::Subtract(_) => tag::OP_SUBTRACT,
		InfixOperator::Multiply(_) => tag::OP_MULTIPLY,
		InfixOperator::Divide(_) => tag::OP_DIVIDE,
		InfixOperator::Rem(_) => tag::OP_REM,
		InfixOperator::Equal(_) => tag::OP_EQUAL,
		InfixOperator::NotEqual(_) => tag::OP_NOT_EQUAL,
		InfixOperator::LessThan(_) => tag::OP_LESS_THAN,
		InfixOperator::LessThanEqual(_) => tag::OP_LESS_THAN_EQUAL,
		InfixOperator::GreaterThan(_) => tag::OP_GREATER_THAN,
		InfixOperator::GreaterThanEqual(_) => tag::OP_GREATER_THAN_EQUAL,
		InfixOperator::And(_) => tag::OP_AND,
		InfixOperator::Or(_) => tag::OP_OR,
		InfixOperator::Xor(_) => tag::OP_XOR,
		InfixOperator::In(_) => tag::OP_IN,
		InfixOperator::NotIn(_) => tag::OP_NOT_IN,
		InfixOperator::Contains(_) => tag::OP_CONTAINS,
		InfixOperator::As(_) => tag::OP_AS,
		InfixOperator::Assign(_) => tag::OP_ASSIGN,
		InfixOperator::Call(_) => tag::OP_CALL,
		InfixOperator::AccessNamespace(_) => tag::OP_ACCESS_NAMESPACE,
		InfixOperator::AccessTable(_) => tag::OP_ACCESS_TABLE,
		InfixOperator::TypeAscription(_) => tag::OP_TYPE_ASCRIPTION,
	});
}

fn write_unresolved_id(buf: &mut FingerprintBuffer, id: &UnresolvedSchemaIdentifier<'_>) {
	for ns in &id.namespace {
		buf.write_str(ns.text());
	}
	buf.write_str(id.name.text());
}

fn write_column_id(buf: &mut FingerprintBuffer, col: &MaybeQualifiedColumnIdentifier<'_>) {
	buf.write_str(col.name.text());
}

fn write_function_id(buf: &mut FingerprintBuffer, func: &MaybeQualifiedFunctionIdentifier<'_>) {
	for ns in &func.namespaces {
		buf.write_str(ns.text());
	}
	buf.write_str(func.name.text());
}

fn write_using_clause(buf: &mut FingerprintBuffer, clause: &AstUsingClause<'_>) {
	buf.write_u16(clause.pairs.len() as u16);
	for pair in &clause.pairs {
		fingerprint_ast(buf, &pair.first);
		fingerprint_ast(buf, &pair.second);
		buf.write_u8(pair.connector.map_or(0, |c| c as u8 + 1));
	}
}

fn write_match_arm(buf: &mut FingerprintBuffer, arm: &AstMatchArm<'_>) {
	match arm {
		AstMatchArm::Value {
			pattern,
			guard,
			result,
		} => {
			buf.write_u8(0x01);
			fingerprint_ast(buf, pattern);
			if let Some(g) = guard {
				fingerprint_ast(buf, g);
			}
			fingerprint_ast(buf, result);
		}
		AstMatchArm::IsVariant {
			namespace,
			sumtype_name,
			variant_name,
			destructure,
			guard,
			result,
		} => {
			buf.write_u8(0x02);
			if let Some(ns) = namespace {
				buf.write_str(ns.text());
			}
			buf.write_str(sumtype_name.text());
			buf.write_str(variant_name.text());
			if let Some(d) = destructure {
				for field in &d.fields {
					buf.write_str(field.text());
				}
			}
			if let Some(g) = guard {
				fingerprint_ast(buf, g);
			}
			fingerprint_ast(buf, result);
		}
		AstMatchArm::Variant {
			variant_name,
			destructure,
			guard,
			result,
		} => {
			buf.write_u8(0x03);
			buf.write_str(variant_name.text());
			if let Some(d) = destructure {
				for field in &d.fields {
					buf.write_str(field.text());
				}
			}
			if let Some(g) = guard {
				fingerprint_ast(buf, g);
			}
			fingerprint_ast(buf, result);
		}
		AstMatchArm::Condition {
			condition,
			guard,
			result,
		} => {
			buf.write_u8(0x04);
			fingerprint_ast(buf, condition);
			if let Some(g) = guard {
				fingerprint_ast(buf, g);
			}
			fingerprint_ast(buf, result);
		}
		AstMatchArm::Else {
			result,
		} => {
			buf.write_u8(0x05);
			fingerprint_ast(buf, result);
		}
	}
}

fn write_optional_returning(buf: &mut FingerprintBuffer, returning: &Option<Vec<Ast<'_>>>) {
	match returning {
		Some(nodes) => {
			buf.write_u8(1);
			fingerprint_ast_slice(buf, nodes);
		}
		None => buf.write_u8(0),
	}
}
