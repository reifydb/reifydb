// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::params::Params;

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

pub(crate) mod callable;
pub(crate) mod exec;
pub mod executor;
pub mod flow_lineage;
pub mod instruction;
pub mod services;
pub mod stack;
#[allow(clippy::module_inception)]
pub mod vm;
pub mod volcano;
