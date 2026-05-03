// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use std::env;

pub fn emit_target_cfg() {
	let target = env::var("TARGET").unwrap_or_default();

	let (reifydb_target, single_threaded) = if env::var("REIFYDB_DST").ok().is_some_and(|v| v == "1") {
		("dst", true)
	} else if target.contains("wasm32") && target.contains("wasi") {
		("wasi", true)
	} else if target.contains("wasm32") {
		("wasm", true)
	} else {
		("native", false)
	};

	println!("cargo::rustc-check-cfg=cfg(reifydb_target, values(\"native\", \"wasm\", \"wasi\", \"dst\"))");
	println!("cargo::rustc-check-cfg=cfg(reifydb_single_threaded)");
	println!("cargo:rustc-cfg=reifydb_target=\"{}\"", reifydb_target);
	if single_threaded {
		println!("cargo:rustc-cfg=reifydb_single_threaded");
	}
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=REIFYDB_DST");
}
