// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::params::Params;

#[derive(Debug)]
pub struct Admin<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Command<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Query<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Subscription<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Test<'a> {
	pub rql: &'a str,
	pub params: Params,
}

pub(crate) mod exec;
pub mod executor;
pub mod instruction;
pub(crate) mod scalar;
pub mod services;
pub mod stack;
#[allow(clippy::module_inception)]
pub mod vm;
pub(crate) mod volcano;
