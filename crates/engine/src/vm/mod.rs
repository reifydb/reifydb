// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::auth::Identity;
use reifydb_type::params::Params;

#[derive(Debug)]
pub struct Admin<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

#[derive(Debug)]
pub struct Command<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

#[derive(Debug)]
pub struct Query<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

pub mod executor;
pub mod instruction;
pub(crate) mod interpret;
pub mod services;
pub mod stack;
pub mod vm;
pub(crate) mod volcano;
