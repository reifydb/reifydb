// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn column(frames: &[Frame], name: &str) -> Vec<String> {
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect()
}

#[test]
fn updating_an_optional_dictionary_column_to_none_interns_nothing() {
	// A none must never take a dictionary id, otherwise every update to none burns the finite id space.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::codes FOR utf8 AS uint4");
	t.admin("CREATE TABLE test::t { id: int4, code: Option(utf8) with { dictionary: test::codes } }");
	t.command("INSERT test::t [{ id: 1, code: 'aa' }, { id: 2, code: 'bb' }]");

	t.command("UPDATE test::t { code: none } FILTER { id == 1 }");

	assert_eq!(
		column(&t.query("FROM test::t | sort { id: ASC }"), "code"),
		["none", "bb"],
		"precondition: the updated row reads back as none"
	);
	assert_eq!(
		column(&t.query("FROM test::codes | sort { id: ASC }"), "value"),
		["aa", "bb"],
		"the dictionary must hold only the two interned values, never an entry for the none"
	);
}
