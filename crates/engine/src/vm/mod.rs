// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{params::Params, value::identity::IdentityId};

#[derive(Debug)]
pub struct Admin<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: IdentityId,
}

#[derive(Debug)]
pub struct Command<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: IdentityId,
}

#[derive(Debug)]
pub struct Query<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: IdentityId,
}

pub mod executor;
pub mod instruction;
pub(crate) mod scalar;
pub mod services;
pub mod stack;
pub mod vm;
pub(crate) mod volcano;
