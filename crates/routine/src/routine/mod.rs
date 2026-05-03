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

pub trait Context: Send + Sync + sealed::Sealed {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionKind {
	Scalar,

	Aggregate,

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

pub trait Routine<C: Context>: Send + Sync {
	fn info(&self) -> &RoutineInfo;

	fn return_type(&self, input_types: &[Type]) -> Type;

	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}

	fn propagates_options(&self) -> bool {
		true
	}

	fn execute(&self, ctx: &mut C, args: &Columns) -> Result<Columns, RoutineError>;

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

pub trait Function: for<'a> Routine<context::FunctionContext<'a>> {
	fn kinds(&self) -> &[FunctionKind];

	fn accumulator(&self, _ctx: &mut context::FunctionContext<'_>) -> Option<Box<dyn Accumulator>> {
		None
	}
}

pub trait Procedure: for<'a, 'tx> Routine<context::ProcedureContext<'a, 'tx>> {}

impl<T: ?Sized> Procedure for T where T: for<'a, 'tx> Routine<context::ProcedureContext<'a, 'tx>> {}

pub trait Accumulator: Send + Sync {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError>;
	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError>;
}
