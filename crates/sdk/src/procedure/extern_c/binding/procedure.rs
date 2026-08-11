// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{config::Config, params::Params};

use crate::{error::Result, procedure::extern_c::binding::context::ExternCProcedureContext};

pub trait ExternCProcedureMetadata {
	const NAME: &'static str;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;
}

pub trait ExternCProcedure: 'static {
	fn new(config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn call(&mut self, ctx: &mut ExternCProcedureContext, params: Params) -> Result<()>;
}

pub trait ExternCProcedureWithMetadata: ExternCProcedure + ExternCProcedureMetadata {}
impl<T> ExternCProcedureWithMetadata for T where T: ExternCProcedure + ExternCProcedureMetadata {}
