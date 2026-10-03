// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod context;
pub mod error;
pub mod registry;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use error::RoutineError;
use reifydb_core::value::column::view::group_by::{GroupId, GroupRows};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};
use serde::{Deserialize, Serialize};

mod sealed {
	pub trait Sealed {}
}

pub trait Context: Send + Sync + sealed::Sealed {
	type Output;

	fn call<R: Routine<Self> + ?Sized>(
		routine: &R,
		ctx: &mut Self,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<Self::Output, RoutineError>
	where
		Self: Sized;
}

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

	fn return_type(&self, input_types: &[ValueType]) -> ValueType;

	fn propagates_options(&self) -> bool {
		true
	}

	fn attaches_row_metadata(&self) -> bool {
		true
	}

	fn execute(&self, ctx: &mut C, args: &[(FieldRef, ArrayRef)]) -> Result<C::Output, RoutineError>;

	fn call(&self, ctx: &mut C, args: &[(FieldRef, ArrayRef)]) -> Result<C::Output, RoutineError> {
		C::call(self, ctx, args)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunctionCapability {
	Retractable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralKind {
	None,
	Bool,
	Number,
	Text,
	Temporal,
	Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiteralArgument {
	pub kind: LiteralKind,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
	Exact(usize),
	Range(usize, usize),
	AtLeast(usize),
	Any,
}

impl Arity {
	pub fn check(self, function: &Fragment, actual: usize) -> Result<(), RoutineError> {
		let (accepted, expected) = match self {
			Arity::Exact(count) => (actual == count, count),
			Arity::Range(min, max) => ((min..=max).contains(&actual), min),
			Arity::AtLeast(min) => (actual >= min, min),
			Arity::Any => (true, 0),
		};
		if accepted {
			return Ok(());
		}
		Err(RoutineError::FunctionArityMismatch {
			function: function.clone(),
			expected,
			actual,
		})
	}
}

pub trait Function: for<'a> Routine<context::FunctionContext<'a>> {
	fn kinds(&self) -> &[FunctionKind];

	fn arity(&self) -> Arity;

	fn accumulator(
		&self,
		_ctx: &mut context::FunctionContext<'_>,
		_literals: &[LiteralArgument],
	) -> Result<Option<Box<dyn Accumulator>>, RoutineError> {
		Ok(None)
	}

	fn max_literal_arguments(&self) -> usize {
		0
	}

	fn aggregate_capabilities(&self) -> &[AggregateFunctionCapability] {
		&[]
	}

	fn type_argument_positions(&self) -> &[usize] {
		&[]
	}

	fn changes_row_count(&self) -> bool {
		false
	}

	fn has_fixed_return_type(&self) -> bool {
		false
	}
}

pub trait Procedure: for<'a, 'tx> Routine<context::ProcedureContext<'a, 'tx>> {}

impl<T> Procedure for T where T: for<'a, 'tx> Routine<context::ProcedureContext<'a, 'tx>> {}

pub trait Accumulator: Send + Sync {
	fn update(&mut self, args: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<(), RoutineError>;
	fn finalize(&mut self) -> Result<(Vec<GroupId>, (FieldRef, ArrayRef)), RoutineError>;
	fn heap_size(&self) -> usize;

	fn kind_name(&self) -> &'static str {
		"accumulator"
	}

	fn retract(&mut self, _args: &[(FieldRef, ArrayRef)], _groups: &GroupRows) -> Result<(), RoutineError> {
		Err(RoutineError::Unsupported {
			op: "retract",
			accumulator: self.kind_name(),
		})
	}
}
