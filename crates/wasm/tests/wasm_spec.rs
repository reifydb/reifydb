// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod support;

macro_rules! spec_test {
    ($fn_name:ident, $file:expr) => {
        #[test]
        fn $fn_name() {
            support::run_test("spec", $file);
        }
    };
}

spec_test!(address, "address.wast");
spec_test!(align, "align.wast");
// spec_test!(binary, "binary.wast");
// spec_test!(binary_leb128, "binary_leb128.wast");
spec_test!(block, "block.wast");
spec_test!(br, "br.wast");
spec_test!(br_if, "br_if.wast");
spec_test!(br_table, "br_table.wast");
// spec_test!(bulk, "bulk.wast");
// Ignored: recursive calls in spec test overflow host stack before engine call-depth limit triggers.
// TODO: fix call-depth tracking to trap before host stack overflow.
#[test]
#[ignore]
fn call() {
    support::run_test("spec", "call.wast");
}
#[test]
#[ignore]
fn call_indirect() {
    support::run_test("spec", "call_indirect.wast");
}

// Ignored: multi-module export lookup not yet implemented.
#[test]
#[ignore]
fn comments() {
    support::run_test("spec", "comments.wast");
}
spec_test!(r#const, "const.wast");
spec_test!(conversions, "conversions.wast");
// spec_test!(custom);
// spec_test!(data);
// spec_test!(elem, "elem.wast");
// spec_test!(endianness, "endianness.wast");
// spec_test!(exports, "exports.wast");
spec_test!(f32, "f32.wast");
spec_test!(f32_bitwise, "f32_bitwise.wast");
spec_test!(f32_cmp, "f32_cmp.wast");
spec_test!(f64, "f64.wast");
spec_test!(f64_bitwise, "f64_bitwise.wast");
spec_test!(f64_cmp, "f64_cmp.wast");
// spec_test!(fac, "fac.wast");
// spec_test!(float_exprs, "float_exprs.wast");
spec_test!(float_literals, "float_literals.wast");
spec_test!(float_memory, "float_memory.wast");
spec_test!(float_misc, "float_misc.wast");
spec_test!(forward, "forward.wast");
spec_test!(func, "func.wast");
// spec_test!(func_ptrs);
// spec_test!(global);
spec_test!(i32, "i32.wast");
spec_test!(i64, "i64.wast");
// spec_test!(r#if, "if.wast");
// spec_test!(imports);
spec_test!(inline_module, "inline_module.wast");
spec_test!(int_exprs, "int_exprs.wast");
spec_test!(int_literals, "int_literals.wast");
// spec_test!(labels, "labels.wast");
// spec_test!(left_to_right, "left_to_right.wast");
// spec_test!(linking);
spec_test!(load, "load.wast");
spec_test!(local_get, "local_get.wast");
spec_test!(local_set, "local_set.wast");
spec_test!(local_tee, "local_tee.wast");
spec_test!(r#loop, "loop.wast");
// spec_test!(memory, "memory.wast");
// spec_test!(memory_copy);
// spec_test!(memory_fill);
// spec_test!(memory_grow, "memory_grow.wast");
// spec_test!(memory_init);
// spec_test!(memory_redundancy);
// spec_test!(memory_size);
// spec_test!(memory_trap);
// spec_test!(names, "names.wast");
// spec_test!(nop, "nop.wast");
spec_test!(obsolete_keywords, "obsolete_keywords.wast");
// spec_test!(ref_func);
// spec_test!(ref_is_null);
// spec_test!(ref_null, "ref_null.wast");
spec_test!(r#return, "return.wast");
// spec_test!(select, "select.wast");
// spec_test!(skip_stack_guard_page);
spec_test!(stack, "stack.wast");
// spec_test!(start);
spec_test!(store, "store.wast");
// spec_test!(switch);
spec_test!(table, "table.wast");
// spec_test!(table_copy);
// spec_test!(table_fill);
// spec_test!(table_get);
// spec_test!(table_grow);
// spec_test!(table_init);
// spec_test!(table_set);
// spec_test!(table_size);
// spec_test!(table_sub);
// spec_test!(token, "token.wast");
// spec_test!(traps);
spec_test!(r#type, "type.wast");
// spec_test!(unreachable, "unreachable.wast");
// spec_test!(unreached_invalid);
// spec_test!(unreached_valid);
// spec_test!(unwind, "unwind.wast");
// spec_test!(utf8_custom_section_id);
// spec_test!(utf8_import_field);
// spec_test!(utf8_import_module);
// spec_test!(utf8_invalid_encoding);
