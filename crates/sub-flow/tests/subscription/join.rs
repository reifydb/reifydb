// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Join parity is #[ignore]'d because PR1's hydrate returns UnsupportedSourceType for
// multi-source flows. Once multi-source hydrate lands, fill in a real test body and
// remove #[ignore]. The placeholder below keeps the operator visible in the test list so it
// is not silently forgotten.

#[ignore]
#[test]
fn join_parity() {
	// Intentional placeholder. Cannot exercise join parity until multi-source hydrate
	// is implemented. See HydrateError::UnsupportedSourceType.
	panic!("join parity placeholder - implement after multi-source hydrate lands");
}
