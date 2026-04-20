// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Dispatch bridge for Binding-driven calls.
//!
//! Synthesizes a `CALL ns::proc()` statement from a `Binding` + caller-supplied `Params`
//! and routes it through the shared `dispatch::dispatch` pipeline. All three transport
//! crates (HTTP, gRPC, WS) call this helper and wrap the returned `(Vec<Frame>, ExecutionMetrics)`
//! in their own response envelope.

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

/// Synthesize and dispatch `CALL {ns}::{proc}()` for a binding; parameters are resolved
/// at execution time from `params` (no `$name` placeholders in the CALL text).
///
/// The caller is responsible for:
/// - Resolving the binding and locating its procedure by `binding.procedure_id`.
/// - Validating `params` against the procedure's declared parameter list (rejecting unknown keys, missing required,
///   type-coercion failures) before calling here.
/// - Building `RequestMetadata` from its transport-specific request.
///
/// Returns `(frames, metrics)` from the engine; each transport wraps it into its own
/// response format based on `binding.format` and reads `metrics.total` / `metrics.fingerprint`
/// for telemetry headers.
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
