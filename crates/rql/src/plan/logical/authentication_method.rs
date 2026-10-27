// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::value::duration::Duration;

use crate::{
	Result,
	ast::ast::{
		AstAlterAuthenticationMethod, AstAuthenticationMethodSetting, AstCreateAuthenticationMethod,
		AstDropAuthenticationMethod,
	},
	duration::{DurationBound, FOREVER, compile_duration},
	nodes::AlterMethodTtl,
	plan::logical::{
		AlterAuthenticationMethodNode, Compiler, CreateAuthenticationMethodNode, DropAuthenticationMethodNode,
		LogicalPlan,
	},
	token::token::{Literal, Token, TokenKind},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_authentication_method(
		&self,
		ast: AstCreateAuthenticationMethod<'bump>,
	) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateAuthenticationMethod(CreateAuthenticationMethodNode {
			name: ast.name,
			ttl: compile_method_ttl(&ast.ttl)?,
			promote_guest: ast.promote_guest.as_ref().is_some_and(is_true),
			properties: compile_method_properties(&ast.settings),
		}))
	}

	pub(crate) fn compile_alter_authentication_method(
		&self,
		ast: AstAlterAuthenticationMethod<'bump>,
	) -> Result<LogicalPlan<'bump>> {
		let ttl = match &ast.ttl {
			Some(token) => AlterMethodTtl::Set(compile_method_ttl(token)?),
			None => AlterMethodTtl::Keep,
		};

		Ok(LogicalPlan::AlterAuthenticationMethod(AlterAuthenticationMethodNode {
			name: ast.name,
			ttl,
			promote_guest: ast.promote_guest.as_ref().map(is_true),
			properties: compile_method_properties(&ast.settings),
		}))
	}

	pub(crate) fn compile_drop_authentication_method(
		&self,
		ast: AstDropAuthenticationMethod<'bump>,
	) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::DropAuthenticationMethod(DropAuthenticationMethodNode {
			name: ast.name,
			if_exists: ast.if_exists,
		}))
	}
}

fn compile_method_ttl(token: &Token<'_>) -> Result<Option<Duration>> {
	if token.fragment.text() == FOREVER {
		return Ok(None);
	}
	compile_duration(token, DurationBound::Positive, "'ttl'").map(Some)
}

fn is_true(token: &Token<'_>) -> bool {
	token.kind == TokenKind::Literal(Literal::True)
}

fn compile_method_properties(settings: &[AstAuthenticationMethodSetting<'_>]) -> BTreeMap<String, String> {
	settings.iter().map(|s| (s.key.text().to_string(), s.value.text().to_string())).collect()
}
