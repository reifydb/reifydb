// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod retention_policy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	// Retention policy discriminators
	pub(crate) const POLICY_KEEP_FOREVER: u8 = 0;
	pub(crate) const POLICY_KEEP_VERSIONS: u8 = 1;

	// Cleanup mode discriminators
	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	// Field indices
	pub(crate) const POLICY_TYPE: usize = 0; // u8: policy discriminator
	pub(crate) const CLEANUP_MODE: usize = 1; // u8: cleanup mode (for applicable policies)
	pub(crate) const VALUE: usize = 2; // u64: numeric value (version count, duration secs, or commit version)

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint1, // policy_type
			Type::Uint1, // cleanup_mode
			Type::Uint8, // value (multi-purpose u64)
		])
	});
}
