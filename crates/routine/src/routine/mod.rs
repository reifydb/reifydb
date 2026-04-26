// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unified routine abstraction.
//!
//! A `Routine` is a named, parameterised, server-side callable that returns
//! `Columns`. Functions and procedures are both routines; they differ only in
//! the execution context they accept:
//!
//! - **Functions** implement `Routine<FunctionContext>`. `FunctionContext` does
//!   not carry a transaction, so the type system guarantees a function cannot
//!   mutate transactional state.
//! - **Procedures** implement `Routine<ProcedureContext>`. `ProcedureContext`
//!   carries `&mut Transaction`, so procedures can read and mutate state.
//!
//! There is one trait declaration. The function-vs-procedure distinction lives
//! entirely in which context the implementor writes against — and in catalog
//! metadata exposed to the user.

pub mod context;
pub mod error;
pub mod registry;

use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	columns::Columns,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_type::{
	util::bitvec::BitVec,
	value::r#type::{Type, input_types::InputTypes},
};
use serde::{Deserialize, Serialize};

pub use context::{FunctionContext, ProcedureContext, RoutineEnv};
pub use error::RoutineError;
pub use registry::{Routines, RoutinesConfigurator};

mod sealed {
	pub trait Sealed {}
}

/// Sealed marker trait for execution contexts. Only `FunctionContext` and
/// `ProcedureContext` implement it. Sealing prevents third parties from
/// introducing a third routine flavour and breaking the registry's invariants.
pub trait Context: Send + Sync + sealed::Sealed {}

/// Function flavour. Lives on the function branch only; procedures don't have
/// a kind discriminator (being a procedure IS the answer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionKind {
	/// Vectorised, 1 row in -> 1 row out.
	Scalar,
	/// Accumulator-based aggregation across N rows -> 1 row per group.
	Aggregate,
	/// Table-valued: 1 row in -> N rows out.
	Generator,
}

#[derive(Debug, Clone)]
pub struct RoutineInfo {
	pub name: String,
	pub description: Option<String>,
}

impl RoutineInfo {
	pub fn new(name: &str) -> Self {
		Self {
			name: name.to_string(),
			description: None,
		}
	}
}

/// The single trait. Implementors pick a context type (`FunctionContext` or
/// `ProcedureContext`); that choice IS the function-vs-procedure declaration
/// and statically determines whether the routine can access the transaction.
pub trait Routine<C: Context>: Send + Sync {
	fn info(&self) -> &RoutineInfo;

	fn return_type(&self, input_types: &[Type]) -> Type;

	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}

	fn propagates_options(&self) -> bool {
		true
	}

	/// Function-only metadata. Default `&[]` — procedures leave this alone.
	/// The default `&[]` is meaningful: a registry user can call `kinds()`
	/// uniformly without having to know whether the routine is a function.
	fn kinds(&self) -> &[FunctionKind] {
		&[]
	}

	/// Execute the routine.
	///
	/// Takes `ctx` by `&mut` so procedure routines can reborrow
	/// `ctx.tx` as `&mut Transaction`. Function routines don't mutate the
	/// context — the `&mut` is a no-op for them, since the env fields are
	/// shared references whose mutability isn't projected through.
	fn execute(&self, ctx: &mut C, args: &Columns) -> Result<Columns, RoutineError>;

	/// Calls the routine, automatically propagating Option columns if
	/// `propagates_options()` returns true. Lifted from the previous
	/// `Function::call` and made generic over `C` — the option-propagation
	/// behaviour is identical for both contexts.
	fn call(&self, ctx: &mut C, args: &Columns) -> Result<Columns, RoutineError> {
		if !self.propagates_options() {
			return self.execute(ctx, args);
		}

		let has_option = args.iter().any(|c| matches!(c.data(), ColumnBuffer::Option { .. }));
		if !has_option {
			return self.execute(ctx, args);
		}

		let mut combined_bv: Option<BitVec> = None;
		let mut unwrapped = Vec::with_capacity(args.len());
		for col in args.iter() {
			let (inner, bv) = col.data().unwrap_option();
			if let Some(bv) = bv {
				combined_bv = Some(match combined_bv {
					Some(existing) => existing.and(bv),
					None => bv.clone(),
				});
			}
			unwrapped.push(ColumnWithName::new(col.name().clone(), inner.clone()));
		}

		// Short-circuit: when all combined values are None, skip the inner routine
		// call entirely to avoid type-validation errors on placeholder inner types.
		if let Some(ref bv) = combined_bv
			&& bv.count_ones() == 0
		{
			let row_count = args.row_count();
			let input_types: Vec<Type> = unwrapped.iter().map(|c| c.data.get_type()).collect();
			let result_type = self.return_type(&input_types);
			let result_data = ColumnBuffer::none_typed(result_type, row_count);
			return Ok(Columns::new(vec![ColumnWithName::new(
				reifydb_type::fragment::Fragment::internal(self.info().name.clone()),
				result_data,
			)]));
		}

		let unwrapped_args = Columns::new(unwrapped);
		let result = self.execute(ctx, &unwrapped_args)?;

		match combined_bv {
			Some(bv) => {
				let wrapped_cols: Vec<ColumnWithName> = result
					.names
					.iter()
					.zip(result.columns.iter())
					.map(|(name, data)| {
						ColumnWithName::new(
							name.clone(),
							ColumnBuffer::Option {
								inner: Box::new(data.clone()),
								bitvec: bv.clone(),
							},
						)
					})
					.collect();
				Ok(Columns::new(wrapped_cols))
			}
			None => Ok(result),
		}
	}

	/// Aggregate accumulator factory. Only function-context routines whose
	/// `kinds()` includes `FunctionKind::Aggregate` should override this.
	fn accumulator(&self, _ctx: &mut C) -> Option<Box<dyn Accumulator>> {
		None
	}
}

/// Aggregate accumulator. Stateful per-group reducer that consumes column
/// batches via `update` and produces final group results via `finalize`.
///
/// (Same trait shape as the legacy `function::Accumulator` — re-declared here
/// so the new `Routine` trait can reference it without depending on the legacy
/// function module. The legacy module re-exports this from step 9 onward.)
pub trait Accumulator: Send + Sync {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError>;
	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError>;
}
