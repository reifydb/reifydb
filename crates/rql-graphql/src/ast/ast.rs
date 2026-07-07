// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use bumpalo::collections::Vec as BumpVec;
use reifydb_rql::bump::{BumpBox, BumpFragment};

use crate::token::token::Token;

#[derive(Debug)]
pub struct AstOperation<'bump> {
	pub token: Token<'bump>,
	pub name: Option<BumpFragment<'bump>>,
	pub selections: BumpVec<'bump, AstSelection<'bump>>,
}

#[derive(Debug)]
pub enum AstSelection<'bump> {
	Field(BumpBox<'bump, AstField<'bump>>),
	// InlineFragment and FragmentSpread could be added here later
}

#[derive(Debug)]
pub struct AstField<'bump> {
	pub token: Token<'bump>,
	pub alias: Option<BumpFragment<'bump>>,
	pub name: BumpFragment<'bump>,
	pub arguments: Option<BumpVec<'bump, AstArgument<'bump>>>,
	pub selections: Option<BumpVec<'bump, AstSelection<'bump>>>,
}

#[derive(Debug)]
pub struct AstArgument<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
	pub value: AstValue<'bump>,
}

#[derive(Debug)]
pub enum AstValue<'bump> {
	Variable(Token<'bump>),
	Int(Token<'bump>),
	Float(Token<'bump>),
	String(Token<'bump>),
	Boolean(Token<'bump>),
	Enum(Token<'bump>),
	List(BumpVec<'bump, AstValue<'bump>>),
	Object(BumpVec<'bump, AstObjectField<'bump>>),
	Null(Token<'bump>),
}

#[derive(Debug)]
pub struct AstObjectField<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
	pub value: AstValue<'bump>,
}
