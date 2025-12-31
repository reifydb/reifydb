// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub use reifydb_core::interface::{
	AggregateFunction, AggregateFunctionContext, GeneratorContext, GeneratorFunction, ScalarFunction,
	ScalarFunctionContext,
};
pub use reifydb_type::Result;

pub mod blob;
pub mod flow_node_type;
pub mod generator;
pub mod math;
mod registry;
pub mod text;

pub use registry::{Functions, FunctionsBuilder};
