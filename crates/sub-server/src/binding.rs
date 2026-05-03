// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use std::fmt::Write;

#[cfg(not(reifydb_single_threaded))]
use reifydb_core::{actors::server::Operation, metric::ExecutionMetrics};
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

#[cfg(not(reifydb_single_threaded))]
use crate::{
	dispatch::dispatch,
	execute::ExecuteError,
	interceptor::{RequestContext, RequestMetadata},
	state::AppState,
};

#[cfg(not(reifydb_single_threaded))]
pub async fn dispatch_binding(
	state: &AppState,
	namespace_path: &str,
	procedure_name: &str,
	params: Params,
	identity: IdentityId,
	metadata: RequestMetadata,
) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
	let mut call_text = String::with_capacity(8 + namespace_path.len() + procedure_name.len());
	write!(&mut call_text, "CALL {}::{}()", namespace_path, procedure_name).unwrap();

	let ctx = RequestContext {
		identity,
		operation: Operation::Command,
		rql: call_text,
		params,
		metadata,
	};

	dispatch(state, ctx).await
}
