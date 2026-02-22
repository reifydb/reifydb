// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod support;

macro_rules! spec_test {
	($fn_name:ident, $file:expr) => {
		#[test]
		fn $fn_name() {
			std::thread::Builder::new()
				.stack_size(64 * 1024 * 1024)
				.spawn(|| support::run_test("spec", $file))
				.expect("failed to spawn test thread")
				.join()
				.unwrap_or_else(|e| std::panic::resume_unwind(e));
		}
	};
}

spec_test!(address, "address.wast");
spec_test!(align, "align.wast");
spec_test!(binary, "binary.wast");
spec_test!(binary_leb128, "binary_leb128.wast");
spec_test!(block, "block.wast");
spec_test!(br, "br.wast");
spec_test!(br_if, "br_if.wast");
spec_test!(br_table, "br_table.wast");
spec_test!(bulk, "bulk.wast");
spec_test!(call, "call.wast");
spec_test!(call_indirect, "call_indirect.wast");

spec_test!(comments, "comments.wast");
spec_test!(r#const, "const.wast");
spec_test!(conversions, "conversions.wast");
spec_test!(custom, "custom.wast");
spec_test!(data, "data.wast");
spec_test!(elem, "elem.wast");
spec_test!(endianness, "endianness.wast");
spec_test!(exports, "exports.wast");
spec_test!(f32, "f32.wast");
spec_test!(f32_bitwise, "f32_bitwise.wast");
spec_test!(f32_cmp, "f32_cmp.wast");
spec_test!(f64, "f64.wast");
spec_test!(f64_bitwise, "f64_bitwise.wast");
spec_test!(f64_cmp, "f64_cmp.wast");
spec_test!(fac, "fac.wast");
spec_test!(float_exprs, "float_exprs.wast");
spec_test!(float_literals, "float_literals.wast");
spec_test!(float_memory, "float_memory.wast");
spec_test!(float_misc, "float_misc.wast");
spec_test!(forward, "forward.wast");
spec_test!(func, "func.wast");
spec_test!(func_ptrs, "func_ptrs.wast");
spec_test!(global, "global.wast");
spec_test!(i32, "i32.wast");
spec_test!(i64, "i64.wast");
spec_test!(r#if, "if.wast");
spec_test!(imports, "imports.wast");
spec_test!(inline_module, "inline_module.wast");
spec_test!(int_exprs, "int_exprs.wast");
spec_test!(int_literals, "int_literals.wast");
spec_test!(labels, "labels.wast");
spec_test!(left_to_right, "left_to_right.wast");
spec_test!(linking, "linking.wast");
spec_test!(load, "load.wast");
spec_test!(local_get, "local_get.wast");
spec_test!(local_set, "local_set.wast");
spec_test!(local_tee, "local_tee.wast");
spec_test!(r#loop, "loop.wast");
spec_test!(memory, "memory.wast");
spec_test!(memory_copy, "memory_copy.wast");
spec_test!(memory_fill, "memory_fill.wast");
spec_test!(memory_grow, "memory_grow.wast");
spec_test!(memory_init, "memory_init.wast");
spec_test!(memory_redundancy, "memory_redundancy.wast");
spec_test!(memory_size, "memory_size.wast");
spec_test!(memory_trap, "memory_trap.wast");
spec_test!(names, "names.wast");
spec_test!(nop, "nop.wast");
spec_test!(obsolete_keywords, "obsolete_keywords.wast");
spec_test!(ref_func, "ref_func.wast");
spec_test!(ref_is_null, "ref_is_null.wast");
spec_test!(ref_null, "ref_null.wast");
spec_test!(r#return, "return.wast");
spec_test!(select, "select.wast");
spec_test!(skip_stack_guard_page, "skip_stack_guard_page.wast");
spec_test!(stack, "stack.wast");
spec_test!(start, "start.wast");
spec_test!(store, "store.wast");
spec_test!(switch, "switch.wast");
spec_test!(table, "table.wast");
spec_test!(table_copy, "table_copy.wast");
spec_test!(table_fill, "table_fill.wast");
spec_test!(table_get, "table_get.wast");
spec_test!(table_grow, "table_grow.wast");
spec_test!(table_init, "table_init.wast");
spec_test!(table_set, "table_set.wast");
spec_test!(table_size, "table_size.wast");
spec_test!(table_sub, "table_sub.wast");
spec_test!(token, "token.wast");
spec_test!(traps, "traps.wast");
spec_test!(r#type, "type.wast");
spec_test!(unreachable, "unreachable.wast");
spec_test!(unreached_invalid, "unreached-invalid.wast");
spec_test!(unreached_valid, "unreached-valid.wast");
spec_test!(unwind, "unwind.wast");
spec_test!(utf8_custom_section_id, "utf8_custom_section_id.wast");
spec_test!(utf8_import_field, "utf8_import_field.wast");
spec_test!(utf8_import_module, "utf8_import_module.wast");
spec_test!(utf8_invalid_encoding, "utf8_invalid_encoding.wast");
