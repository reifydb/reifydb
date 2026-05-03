// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::util::ioc::IocContainer;
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, value::identity::IdentityId};

use super::{Context, sealed};

pub struct FunctionContext<'a> {
	pub fragment: Fragment,
	pub identity: IdentityId,
	pub row_count: usize,
	pub runtime_context: &'a RuntimeContext,
}

impl sealed::Sealed for FunctionContext<'_> {}
impl Context for FunctionContext<'_> {}

pub struct ProcedureContext<'a, 'tx> {
	pub fragment: Fragment,
	pub identity: IdentityId,
	pub row_count: usize,
	pub runtime_context: &'a RuntimeContext,
	pub tx: &'a mut Transaction<'tx>,
	pub params: &'a Params,
	pub catalog: &'a Catalog,
	pub ioc: &'a IocContainer,
}

impl sealed::Sealed for ProcedureContext<'_, '_> {}
impl Context for ProcedureContext<'_, '_> {}
