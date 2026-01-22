//! Build utilities for ReifyDB target detection.
//!
//! This crate provides a single function to emit the `reifydb_target` cfg
//! based on the compilation target. Add this crate as a build-dependency
//! and call `emit_target_cfg()` from your build.rs.
//!
//! # Supported Targets
//! - `native` - Default for non-WASM targets
//! - `wasm` - For wasm32 targets
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
/// ```
pub fn emit_target_cfg() {
	let target = env::var("TARGET").unwrap_or_default();

	let reifydb_target = if target.contains("wasm32") {
		"wasm"
	} else {
		// Default to native for all non-wasm targets
		// Future: could check for DST-specific target/env var here
		"native"
	};

	// Emit the check-cfg directive to tell the compiler about our custom cfg
	println!("cargo::rustc-check-cfg=cfg(reifydb_target, values(\"native\", \"wasm\", \"dst\"))");
	println!("cargo:rustc-cfg=reifydb_target=\"{}\"", reifydb_target);
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=TARGET");
}
