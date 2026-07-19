// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg(reifydb_single_threaded)]

mod common;

use std::{collections::HashMap, sync::Arc};

use common::DstTestContext;
use reifydb_client::{Frame, Params, Value};
use reifydb_core::actors::server::{ServerAuthResponse, ServerResponse};
use reifydb_value::value::identity::IdentityId;

fn frames_of(response: ServerResponse) -> Vec<Frame> {
	match response {
		ServerResponse::Success {
			frames,
			..
		} => frames,
		ServerResponse::EngineError {
			diagnostic,
			..
		} => panic!("unexpected engine error: {} {}", diagnostic.code, diagnostic.message),
	}
}

fn error_code(response: ServerResponse) -> String {
	match response {
		ServerResponse::EngineError {
			diagnostic,
			..
		} => diagnostic.code,
		ServerResponse::Success {
			..
		} => panic!("expected an error, got success"),
	}
}

fn column_value(frames: &[Frame], column: &str) -> Value {
	let frame = frames.first().expect("one frame");
	let col = frame.columns.iter().find(|c| c.name == column).unwrap_or_else(|| panic!("column `{column}`"));
	col.data.get_value(0)
}

fn named_n(n: i32) -> Params {
	Params::Named(Arc::new(HashMap::from([("n".to_string(), Value::Int4(n))])))
}

fn authenticate(ctx: &DstTestContext, token: &str) -> IdentityId {
	match ctx.client.authenticate("token".to_string(), HashMap::from([("token".to_string(), token.to_string())])) {
		ServerAuthResponse::Authenticated {
			identity,
			..
		} => identity,
		ServerAuthResponse::Failed {
			reason,
		} => panic!("authentication failed: {}", reason),
		ServerAuthResponse::Error(e) => panic!("authentication error: {}", e),
		ServerAuthResponse::Challenge {
			..
		} => panic!("unexpected challenge response"),
	}
}

#[test]
fn call_zero_param_binding_returns_procedure_frame() {
	let ctx = DstTestContext::new();
	ctx.db.admin_as_root("CREATE NAMESPACE dst_call", Params::None).unwrap();
	ctx.db.admin_as_root("CREATE PROCEDURE dst_call::greet AS { MAP { result: 42 } }", Params::None).unwrap();
	ctx.db.admin_as_root(
		"CREATE WS BINDING dst_call::greet_ws FOR dst_call::greet WITH { name: \"dst_greet\" }",
		Params::None,
	)
	.unwrap();

	let frames = frames_of(ctx.client.call(ctx.identity, "dst_greet".to_string(), Params::None));
	assert_eq!(frames.len(), 1);
	assert!(frames[0].to_string().contains("42"));
}

#[test]
fn call_passes_named_params_through() {
	let ctx = DstTestContext::new();
	ctx.db.admin_as_root("CREATE NAMESPACE dst_call", Params::None).unwrap();
	ctx.db.admin_as_root("CREATE PROCEDURE dst_call::echo { n: int4 } AS { MAP { out: $n } }", Params::None)
		.unwrap();
	ctx.db.admin_as_root(
		"CREATE WS BINDING dst_call::echo_ws FOR dst_call::echo WITH { name: \"dst_echo\" }",
		Params::None,
	)
	.unwrap();

	// 12345 appears only if the param reached the procedure body over the DST transport.
	let frames = frames_of(ctx.client.call(ctx.identity, "dst_echo".to_string(), named_n(12345)));
	assert!(frames[0].to_string().contains("12345"));
}

#[test]
fn call_missing_required_param_errors() {
	let ctx = DstTestContext::new();
	ctx.db.admin_as_root("CREATE NAMESPACE dst_call", Params::None).unwrap();
	ctx.db.admin_as_root("CREATE PROCEDURE dst_call::echo { n: int4 } AS { MAP { out: $n } }", Params::None)
		.unwrap();
	ctx.db.admin_as_root(
		"CREATE WS BINDING dst_call::echo_ws FOR dst_call::echo WITH { name: \"dst_echo\" }",
		Params::None,
	)
	.unwrap();

	assert_eq!(error_code(ctx.client.call(ctx.identity, "dst_echo".to_string(), Params::None)), "INVALID_PARAMS");
}

#[test]
fn call_unknown_binding_errors() {
	let ctx = DstTestContext::new();
	assert_eq!(error_code(ctx.client.call(ctx.identity, "no_such_binding".to_string(), Params::None)), "NOT_FOUND");
}

#[test]
fn call_observes_the_authenticated_caller_identity() {
	let ctx = DstTestContext::new();
	ctx.db.admin_as_root("CREATE USER alice", Params::None).unwrap();
	ctx.db.admin_as_root("CREATE AUTHENTICATION FOR alice { method: token; token: 'alice-tok' }", Params::None)
		.unwrap();
	ctx.db.admin_as_root("CREATE USER bob", Params::None).unwrap();
	ctx.db.admin_as_root("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-tok' }", Params::None)
		.unwrap();
	ctx.db.admin_as_root("CREATE NAMESPACE dst_ident", Params::None).unwrap();
	ctx.db.admin_as_root("CREATE PROCEDURE dst_ident::whoami AS { MAP { caller: identity::id() } }", Params::None)
		.unwrap();
	// Non-privileged callers need a call policy; `filter { true }` admits any authenticated identity.
	ctx.db.admin_as_root(
		"CREATE PROCEDURE POLICY ON dst_ident::whoami { call: { filter { true } } }",
		Params::None,
	)
	.unwrap();
	ctx.db.admin_as_root(
		"CREATE WS BINDING dst_ident::whoami_ws FOR dst_ident::whoami WITH { name: \"dst_whoami\" }",
		Params::None,
	)
	.unwrap();

	let alice = authenticate(&ctx, "alice-tok");
	let bob = authenticate(&ctx, "bob-tok");
	assert_ne!(alice, bob, "distinct users must have distinct ids");

	let observed =
		column_value(&frames_of(ctx.client.call(alice, "dst_whoami".to_string(), Params::None)), "caller");
	assert_eq!(observed, Value::IdentityId(alice), "procedure must observe alice as the caller");

	let observed = column_value(&frames_of(ctx.client.call(bob, "dst_whoami".to_string(), Params::None)), "caller");
	assert_eq!(observed, Value::IdentityId(bob), "procedure must observe bob as the caller");
}
