// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::fingerprint::StatementFingerprint;

use super::{
	request::{RequestFingerprint, fingerprint_request},
	statement::fingerprint_statement,
};
use crate::{ast::parse_str, bump::Bump};

fn fp(query: &str) -> StatementFingerprint {
	let bump = Bump::new();
	let stmts = parse_str(&bump, query).unwrap();
	assert_eq!(stmts.len(), 1, "expected single statement for: {query}");
	fingerprint_statement(&stmts[0])
}

fn fp_request(query: &str) -> (Vec<StatementFingerprint>, RequestFingerprint) {
	let bump = Bump::new();
	let stmts = parse_str(&bump, query).unwrap();
	let fps: Vec<_> = stmts.iter().map(|s| fingerprint_statement(s)).collect();
	let req = fingerprint_request(&fps);
	(fps, req)
}

#[test]
fn same_pattern_different_numbers() {
	assert_eq!(fp("FROM users FILTER {id == 42}"), fp("FROM users FILTER {id == 99}"),);
}

#[test]
fn same_pattern_different_strings() {
	assert_eq!(fp("FROM users FILTER {name == 'alice'}"), fp("FROM users FILTER {name == 'bob'}"),);
}

#[test]
fn same_pattern_different_in_list_values() {
	assert_eq!(fp("FROM users FILTER {id IN [1, 2, 3]}"), fp("FROM users FILTER {id IN [4, 5, 6]}"),);
}

#[test]
fn same_insert_pattern() {
	assert_eq!(fp("INSERT users [{id: 1, name: 'alice'}]"), fp("INSERT users [{id: 2, name: 'bob'}]"),);
}

#[test]
fn different_tables() {
	assert_ne!(fp("FROM users FILTER {id == 1}"), fp("FROM orders FILTER {id == 1}"),);
}

#[test]
fn different_operators() {
	assert_ne!(fp("FROM users FILTER {id == 1}"), fp("FROM users FILTER {id != 1}"),);
}

#[test]
fn different_columns() {
	assert_ne!(fp("FROM users FILTER {id == 1}"), fp("FROM users FILTER {name == 1}"),);
}

#[test]
fn different_statement_types() {
	assert_ne!(fp("FROM users FILTER {id == 1}"), fp("DELETE users FILTER {id == 1}"),);
}

#[test]
fn different_literal_types() {
	assert_ne!(fp("FROM users FILTER {x == 1}"), fp("FROM users FILTER {x == 'hello'}"),);
}

#[test]
fn request_fingerprint_single_statement() {
	let (fps, _req) = fp_request("FROM users FILTER {id == 1}");
	assert_eq!(fps.len(), 1);
}

#[test]
fn request_fingerprint_same_pattern() {
	let (_, req1) = fp_request("FROM users FILTER {id == 1}; FROM orders FILTER {id == 1}");
	let (_, req2) = fp_request("FROM users FILTER {id == 99}; FROM orders FILTER {id == 99}");
	assert_eq!(req1, req2);
}

#[test]
fn request_fingerprint_different_order() {
	let (_, req1) = fp_request("FROM users FILTER {id == 1}; FROM orders FILTER {id == 1}");
	let (_, req2) = fp_request("FROM orders FILTER {id == 1}; FROM users FILTER {id == 1}");
	assert_ne!(req1, req2);
}

#[test]
fn fingerprint_is_deterministic() {
	let a = fp("FROM users FILTER {id == 42}");
	let b = fp("FROM users FILTER {id == 42}");
	assert_eq!(a, b);
}

#[test]
fn statement_fingerprint_bytes_roundtrip() {
	let fp1 = fp("FROM users FILTER {id == 1}");
	let bytes = fp1.to_le_bytes();
	let fp2 = StatementFingerprint::from_le_bytes(bytes);
	assert_eq!(fp1, fp2);
}

#[test]
fn request_fingerprint_bytes_roundtrip() {
	let (_, req1) = fp_request("FROM users FILTER {id == 1}");
	let bytes = req1.to_le_bytes();
	let req2 = RequestFingerprint::from_le_bytes(bytes);
	assert_eq!(req1, req2);
}
