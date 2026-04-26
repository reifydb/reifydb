// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::util::ioc::IocContainer;
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::identity::IdentityId,
};

/// Read-only execution facets present on EVERY routine context.
///
/// Strict subset of what both function and procedure contexts genuinely share.
/// Catalog/IOC/self-references to the routine registry are NOT here — those
/// are procedure-only fields.
///
/// **Invariant.** This struct does NOT and MUST NOT carry `&mut Transaction`.
/// The transaction lives only on `ProcedureContext`. Adding it here would
/// silently break the static purity guarantee for functions.
pub struct RoutineEnv<'a> {
	pub fragment: Fragment,
	pub identity: IdentityId,
	pub row_count: usize,
	pub runtime_context: &'a RuntimeContext,
}

/// Execution context for a function.
///
/// A function impl writes `impl Routine<FunctionContext> for Foo`. The trait's
/// `execute` signature has no way to receive `&mut Transaction`, so a function
/// is guaranteed at the type level to be unable to mutate transactional state.
///
/// Functions don't get `catalog`/`ioc` — they're pure operations on column
/// data. If a future "function" needs catalog access, it should be a
/// procedure instead.
pub struct FunctionContext<'a> {
	pub env: RoutineEnv<'a>,
}

impl super::sealed::Sealed for FunctionContext<'_> {}
impl super::Context for FunctionContext<'_> {}

/// Execution context for a procedure.
///
/// A procedure impl writes `impl Routine<ProcedureContext> for Foo`. The
/// transaction is reachable via `ctx.tx`. Procedures get the catalog and the
/// IOC container in addition to the shared env, plus access to user-supplied
/// parameters as the legacy `Params` view.
pub struct ProcedureContext<'a, 'tx> {
	pub env: RoutineEnv<'a>,
	pub tx: &'a mut Transaction<'tx>,
	pub params: &'a Params,
	pub catalog: &'a Catalog,
	pub ioc: &'a IocContainer,
}

impl super::sealed::Sealed for ProcedureContext<'_, '_> {}
impl super::Context for ProcedureContext<'_, '_> {}
