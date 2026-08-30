// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use std::env;

pub fn emit_target_cfg() {
	let target = env::var("TARGET").unwrap_or_default();

	let reifydb_target = if target.contains("wasm32") && target.contains("wasi") {
		"wasi"
	} else if target.contains("wasm32") {
		"wasm"
	} else {
		"host"
	};

	let dst = env::var("REIFYDB_DST").ok().is_some_and(|v| v == "1");
	let single_threaded = dst || reifydb_target != "host";

	println!("cargo::rustc-check-cfg=cfg(reifydb_target, values(\"host\", \"wasm\", \"wasi\"))");
	println!("cargo::rustc-check-cfg=cfg(reifydb_dst)");
	println!("cargo::rustc-check-cfg=cfg(reifydb_single_threaded)");
	println!("cargo::rustc-check-cfg=cfg(reifydb_assertions)");
	println!("cargo::rustc-check-cfg=cfg(loom)");
	println!("cargo:rustc-cfg=reifydb_target=\"{}\"", reifydb_target);
	if dst {
		println!("cargo:rustc-cfg=reifydb_dst");
	}
	if single_threaded {
		println!("cargo:rustc-cfg=reifydb_single_threaded");
	}
	let assertions = env::var("REIFYDB_ASSERTIONS").ok().is_some_and(|v| v == "1")
		|| env::var("PROFILE").is_ok_and(|p| p == "debug");
	if assertions {
		println!("cargo:rustc-cfg=reifydb_assertions");
	}
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=REIFYDB_DST");
	println!("cargo:rerun-if-env-changed=REIFYDB_ASSERTIONS");
}
