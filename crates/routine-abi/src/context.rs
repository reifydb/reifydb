// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_buffer::NullBuffer;
use arrow_schema::FieldRef;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	util::ioc::IocContainer,
	value::column::{
		factory::none_typed,
		nulls::{split_nulls, with_nulls},
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	util::bitmap,
	value::{column_view::ColumnView, identity::IdentityId, value_type::ValueType},
};

use super::{Context, Routine, error::RoutineError, sealed};

pub struct FunctionContext<'a> {
	pub fragment: Fragment,
	pub identity: IdentityId,
	pub row_count: usize,
	pub runtime_context: &'a RuntimeContext,
}

impl sealed::Sealed for FunctionContext<'_> {}
impl Context for FunctionContext<'_> {
	type Output = (FieldRef, ArrayRef);

	fn call<R: Routine<Self> + ?Sized>(
		routine: &R,
		ctx: &mut Self,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		if !routine.propagates_options() {
			return routine.execute(ctx, args);
		}

		let has_option = args.iter().any(|(field, _)| field.is_nullable());
		if !has_option {
			return routine.execute(ctx, args);
		}

		let mut combined: Option<NullBuffer> = None;
		let mut unwrapped = Vec::with_capacity(args.len());
		for column in args {
			let (inner, nulls) = split_nulls(column.clone())?;
			if let Some(nulls) = nulls {
				combined = Some(match combined {
					Some(existing) => bitmap::and_nulls(&existing, &nulls),
					None => nulls,
				});
			}
			unwrapped.push(inner);
		}

		if let Some(ref nulls) = combined
			&& nulls.null_count() == nulls.len()
		{
			let row_count = args.first().map_or(0, |(_, array)| array.len());
			let mut input_types: Vec<ValueType> = Vec::with_capacity(unwrapped.len());
			for column in &unwrapped {
				input_types.push(ColumnView::try_from(column)?.get_type());
			}
			let result_type = routine.return_type(&input_types);
			return Ok(none_typed(&routine.info().name, result_type, row_count));
		}

		let result = routine.execute(ctx, &unwrapped)?;

		match combined {
			Some(nulls) => {
				let validity = bitmap::resize(nulls.inner(), result.1.len());
				Ok(with_nulls(result, NullBuffer::new(validity))?)
			}
			None => Ok(result),
		}
	}
}

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
impl Context for ProcedureContext<'_, '_> {
	type Output = RecordBatch;

	fn call<R: Routine<Self> + ?Sized>(
		routine: &R,
		ctx: &mut Self,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<RecordBatch, RoutineError> {
		routine.execute(ctx, args)
	}
}
