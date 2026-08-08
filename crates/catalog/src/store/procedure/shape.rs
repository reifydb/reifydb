// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

pub(crate) const VARIANT_RQL: u8 = 0;
pub(crate) const VARIANT_TEST: u8 = 1;

pub(crate) const TRIGGER_CALL: u8 = 0;
pub(crate) const TRIGGER_EVENT: u8 = 1;

catalog_shape! {
	pub(crate) procedure {
		id: u64,
		namespace: u64,
		name: utf8,
		variant: u8,
		body: utf8,
		trigger_kind: u8,
		trigger_variant_sumtype: u64,
		trigger_variant_index: u16,
		return_type: utf8,
	}

	pub(crate) namespace_procedure {
		id: u64,
		name: utf8,
	}

	pub(crate) procedure_param {
		procedure_id: u64,
		index: u16,
		name: utf8,
		type_constraint: utf8,
	}
}
