#[test]
fn class_rules_hold_at_compile_time() {
	// without this, a loosened bound lets an operator reach state its class forbids and every other test still passes
	trybuild::TestCases::new().compile_fail("tests/class_compile_fail/*.rs");
}
