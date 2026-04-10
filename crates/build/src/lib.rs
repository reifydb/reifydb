// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

//! Build utilities for ReifyDB target detection.
//!
//! This crate provides a single function to emit the `reifydb_target` cfg
//! based on the compilation target. Add this crate as a build-dependency
//! and call `emit_target_cfg()` from your build.rs.
//!
//! # Supported Targets
//! - `native` - Default for non-WASM targets
//! - `wasm` - For wasm32-unknown-unknown (JS-WASM) targets
//! - `wasi` - For wasm32-wasip1 (WASI) targets
//! - `dst` - (Future) Deterministic software testing

use std::env;

/// Emit the `reifydb_target` cfg based on the current compilation target.
///
/// Call this from your crate's `build.rs`:
/// ```ignore
/// fn main() {
///     reifydb_build::emit_target_cfg();
/// }
/// ```
///
/// Then use in code:
/// ```ignore
/// #[cfg(reifydb_target = "native")]
/// fn native_only() { }
///
/// #[cfg(reifydb_target = "wasm")]
/// fn wasm_only() { }
///
/// #[cfg(reifydb_target = "wasi")]
/// fn wasi_only() { }
/// ```
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

	// Emit the check-cfg directive to tell the compiler about our custom cfgs
	println!("cargo::rustc-check-cfg=cfg(reifydb_target, values(\"native\", \"wasm\", \"wasi\", \"dst\"))");
	println!("cargo::rustc-check-cfg=cfg(reifydb_single_threaded)");
	println!("cargo:rustc-cfg=reifydb_target=\"{}\"", reifydb_target);
	if single_threaded {
		println!("cargo:rustc-cfg=reifydb_single_threaded");
	}
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=REIFYDB_DST");
}
