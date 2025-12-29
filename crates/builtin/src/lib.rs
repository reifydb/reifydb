// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
