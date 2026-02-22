// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod support;

macro_rules! wast_test {
	($fn_name:ident, $file:expr) => {
		#[test]
		fn $fn_name() {
			std::thread::Builder::new()
				.stack_size(64 * 1024 * 1024)
				.spawn(|| support::run_test("wast", $file))
				.expect("failed to spawn test thread")
				.join()
				.unwrap_or_else(|e| std::panic::resume_unwind(e));
		}
	};
}

wast_test!(br, "br.wast");
wast_test!(br_table_extern_ref, "br_table_extern_ref.wast");
wast_test!(call_indirect, "call_indirect.wast");
wast_test!(call_indirect_trap, "call_indirect_trap.wast");
wast_test!(call_recursive, "call_recursive.wast");
wast_test!(break_multi_value, "break_multi_value.wast");
wast_test!(float_from_binary, "float_from_binary.wast");
wast_test!(i32_load_8s, "i32_load_8s.wast");
wast_test!(malformed, "malformed.wast");
wast_test!(multi_module, "multi_module.wast");
wast_test!(numeric, "numeric.wast");
wast_test!(return_multiple_values, "return_multiple_values.wast");
wast_test!(select, "select.wast");
wast_test!(swap, "swap.wast");
