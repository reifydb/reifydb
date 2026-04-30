// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod context;
pub mod error;
pub mod registry;

use error::RoutineError;
use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	columns::Columns,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::r#type::{Type, input_types::InputTypes},
};
use serde::{Deserialize, Serialize};

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

/// The generic, function-and-procedure-agnostic contract. Implementors pick a
/// context type (`FunctionContext` or `ProcedureContext`); that choice IS the
/// function-vs-procedure declaration and statically determines whether the
/// routine can access the transaction.
///
/// Function-only concerns (`kinds`, `accumulator`) live on the `Function`
/// sub-trait. Procedures get a marker sub-trait `Procedure` with a blanket
/// impl, so existing procedure impls require no extra boilerplate.
pub trait Routine<C: Context>: Send + Sync {
	fn info(&self) -> &RoutineInfo;

	fn return_type(&self, input_types: &[Type]) -> Type;

	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}

	fn propagates_options(&self) -> bool {
		true
	}

	/// Execute the routine.
	///
	/// Takes `ctx` by `&mut` so procedure routines can reborrow
	/// `ctx.tx` as `&mut Transaction`. Function routines don't mutate the
	/// context  - the `&mut` is a no-op for them, since the env fields are
	/// shared references whose mutability isn't projected through.
	fn execute(&self, ctx: &mut C, args: &Columns) -> Result<Columns, RoutineError>;

	/// Calls the routine, automatically propagating Option columns if
	/// `propagates_options()` returns true. The option-propagation behaviour
	/// is identical for both contexts, hence the shared default.
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
				Fragment::internal(self.info().name.clone()),
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
}

/// Function-specific extension of `Routine`. Carries the kind discriminator
/// and the optional aggregate accumulator factory. Procedures do not see these
/// methods.
pub trait Function: for<'a> Routine<context::FunctionContext<'a>> {
	/// The execution shapes this function supports (Scalar, Aggregate,
	/// Generator). Required: every function declares at least one kind.
	fn kinds(&self) -> &[FunctionKind];

	/// Aggregate accumulator factory. Only functions whose `kinds()` includes
	/// `FunctionKind::Aggregate` need to override this.
	fn accumulator(&self, _ctx: &mut context::FunctionContext<'_>) -> Option<Box<dyn Accumulator>> {
		None
	}
}

/// Procedure marker. Empty: every implementor of
/// `Routine<ProcedureContext<'_, '_>>` is automatically a `Procedure` via the
/// blanket impl below. Exists so `dyn Procedure` is a real type and so
/// procedure-only methods have an obvious home if we add any later.
pub trait Procedure: for<'a, 'tx> Routine<context::ProcedureContext<'a, 'tx>> {}

impl<T: ?Sized> Procedure for T where T: for<'a, 'tx> Routine<context::ProcedureContext<'a, 'tx>> {}

/// Aggregate accumulator. Stateful per-group reducer that consumes column
/// batches via `update` and produces final group results via `finalize`.
pub trait Accumulator: Send + Sync {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError>;
	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError>;
}
