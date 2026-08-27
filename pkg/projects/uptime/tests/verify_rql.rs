// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Regression guard for the RQL procedure constructs that uptime::report_result
// depends on: IF/ELSE branch return, MATCH-as-expression via LET, block-scoped
// LET, Option-typed params accepting none, int arithmetic + int4>=int2, and the
// sum-of-0/1-flag rollup pattern. These parse and compile in reifydb but had no
// runnable test there, so this pins the runtime behavior report_result relies on.
// If a reifydb change breaks any of these, report_result breaks with it.

use reifydb::{Database, IdentityId, Value, WithSubsystem, server, value::params::Params};

fn build() -> Database {
	server::memory().with_flow(|f| f).build().expect("build memory db")
}

fn admin(db: &Database, rql: &str) {
	let r = db.engine().admin_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("admin failed for [{rql}]: {e:?}");
	}
}

fn call(db: &Database, rql: &str, params: Params) -> Result<String, String> {
	let r = db.engine().command_as(IdentityId::root(), rql, params);
	match r.error {
		Some(e) => Err(format!("{e:?}")),
		None => Ok(r.frames.iter().map(|f| f.to_string()).collect::<Vec<_>>().join("\n")),
	}
}

#[test]
fn if_else_branch_returns() {
	let db = build();
	admin(&db, "create namespace test");
	admin(
		&db,
		r#"create procedure test::iff { n: int4 } as { if $n > 10 { map { label: "high" } } else { map { label: "low" } } }"#,
	);
	let hi = call(&db, "CALL test::iff(20)", Params::None);
	let lo = call(&db, "CALL test::iff(5)", Params::None);
	println!("IF high -> {hi:?}");
	println!("IF low  -> {lo:?}");
	assert!(hi.as_ref().map(|s| s.contains("high")).unwrap_or(false), "IF-true branch: {hi:?}");
	assert!(lo.as_ref().map(|s| s.contains("low")).unwrap_or(false), "IF-false branch: {lo:?}");
}

#[test]
fn match_expression_via_let() {
	let db = build();
	admin(&db, "create namespace test");
	admin(
		&db,
		r#"create procedure test::mtch { n: int4 } as { let $l = match { $n > 10 => "high", else => "low" }; map { label: $l } }"#,
	);
	let hi = call(&db, "CALL test::mtch(20)", Params::None);
	let lo = call(&db, "CALL test::mtch(5)", Params::None);
	println!("MATCH high -> {hi:?}");
	println!("MATCH low  -> {lo:?}");
	assert!(hi.as_ref().map(|s| s.contains("high")).unwrap_or(false), "MATCH-expr true: {hi:?}");
	assert!(lo.as_ref().map(|s| s.contains("low")).unwrap_or(false), "MATCH-expr false: {lo:?}");
}

#[test]
fn let_reassign_across_block() {
	let db = build();
	admin(&db, "create namespace test");
	admin(
		&db,
		r#"create procedure test::reassign { n: int4 } as { let $l = "low"; if $n > 10 { let $l = "high" }; map { label: $l } }"#,
	);
	let hi = call(&db, "CALL test::reassign(20)", Params::None);
	// Block-scoped: the inner `let` does NOT reassign the outer var, so report_result
	// must compute conditional values with MATCH expressions, not "set in a branch,
	// read later". If this ever returns "high", that assumption is broken.
	println!("REASSIGN(20) -> {hi:?}  (high=outer-reassign, low=block-scoped)");
	assert!(hi.as_ref().map(|s| s.contains("low")).unwrap_or(false), "expected block-scoped 'low': {hi:?}");
}

#[test]
fn option_param_none_via_map() {
	let db = build();
	admin(&db, "create namespace test");
	admin(&db, "create table test::t { id: int4, v: Option(int4) }");
	// Param declared with a plain (non-Option) type; pass none via a Params map.
	admin(&db, "create procedure test::ins_plain { i: int4, v: int4 } as { insert test::t [{ id: $i, v: $v }] }");
	let mut map = std::collections::HashMap::new();
	map.insert("i".to_string(), Value::Int4(1));
	map.insert("v".to_string(), Value::none());
	let plain = call(&db, "CALL test::ins_plain($i, $v)", Params::from(map));
	println!("Option param (plain type, none) -> {plain:?}");

	// Param declared with an Option type.
	admin(
		&db,
		"create procedure test::ins_opt { i: int4, v: Option(int4) } as { insert test::t [{ id: $i, v: $v }] }",
	);
	let mut map2 = std::collections::HashMap::new();
	map2.insert("i".to_string(), Value::Int4(2));
	map2.insert("v".to_string(), Value::none());
	let opt = call(&db, "CALL test::ins_opt($i, $v)", Params::from(map2));
	println!("Option param (Option type, none) -> {opt:?}");

	let rows = call(&db, "from test::t sort { id: asc }", Params::None);
	println!("rows after inserts -> {rows:?}");
	assert!(plain.is_ok(), "plain-typed param accepting none: {plain:?}");
	assert!(opt.is_ok(), "Option-typed param accepting none: {opt:?}");
	assert!(
		rows.as_ref().map(|s| s.matches("none").count() >= 2).unwrap_or(false),
		"both rows should store v as none: {rows:?}"
	);
}

#[test]
fn multi_arm_match_rollup_and_count_on_empty() {
	let db = build();
	admin(&db, "create namespace test");
	admin(&db, "create table test::mr { monitor_id: int4, status: utf8 }");
	admin(
		&db,
		r#"insert test::mr [{ monitor_id: 1, status: "up" }, { monitor_id: 1, status: "down" }, { monitor_id: 3, status: "up" }, { monitor_id: 5, status: "unknown" }]"#,
	);
	admin(
		&db,
		r#"create procedure test::rollup { m: int4 } as {
			let $ups = from test::mr filter { monitor_id == $m } map { f: match { status == "up" => 1, else => 0 } } aggregate { s: math::sum(f) };
			let $downs = from test::mr filter { monitor_id == $m } map { f: match { status == "down" => 1, else => 0 } } aggregate { s: math::sum(f) };
			map { rollup: match { $ups > 0 and $downs > 0 => "degraded", $downs > 0 => "down", $ups > 0 => "up", else => "unknown" } }
		}"#,
	);
	let mixed = call(&db, "CALL test::rollup(1)", Params::None); // up+down -> degraded
	let up_only = call(&db, "CALL test::rollup(3)", Params::None); // up only -> up
	let unknown = call(&db, "CALL test::rollup(5)", Params::None); // all unknown -> unknown
	println!("rollup mixed(1)    -> {mixed:?}");
	println!("rollup up(3)       -> {up_only:?}");
	println!("rollup unknown(5)  -> {unknown:?}");
	assert!(mixed.as_ref().map(|s| s.contains("degraded")).unwrap_or(false), "mixed: {mixed:?}");
	assert!(up_only.as_ref().map(|s| s.contains("up")).unwrap_or(false), "up-only: {up_only:?}");
	assert!(unknown.as_ref().map(|s| s.contains("unknown")).unwrap_or(false), "unknown: {unknown:?}");
}

#[test]
fn field_arithmetic_and_int_comparison() {
	let db = build();
	admin(&db, "create namespace test");
	admin(&db, "create table test::mon { id: int4, thr: int2 }");
	admin(&db, "insert test::mon [{ id: 1, thr: 2 }]");
	admin(&db, r#"create table test::rs { id: int4, cf: int4, status: utf8 }"#);
	admin(&db, r#"insert test::rs [{ id: 1, cf: 1, status: "up" }]"#);
	admin(
		&db,
		r#"create procedure test::compute { id: int4, success: bool } as {
			let $cf = from test::rs filter { id == $id } map { cf };
			let $status = from test::rs filter { id == $id } map { status };
			let $thr = from test::mon filter { id == $id } map { thr };
			let $failures = match { $success => 0, else => $cf + 1 };
			let $rstatus = match { $success => "up", $failures >= $thr => "down", else => $status };
			map { failures: $failures, rstatus: $rstatus }
		}"#,
	);
	let fail = call(&db, "CALL test::compute(1, false)", Params::None); // cf 1 -> failures 2 >= thr 2 -> down
	let ok = call(&db, "CALL test::compute(1, true)", Params::None); // failures 0 -> up
	println!("compute fail -> {fail:?}");
	println!("compute ok   -> {ok:?}");
	assert!(fail.as_ref().map(|s| s.contains('2') && s.contains("down")).unwrap_or(false), "fail-branch: {fail:?}");
	assert!(ok.as_ref().map(|s| s.contains("up")).unwrap_or(false), "ok-branch: {ok:?}");
}
