// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod context;
pub mod dictionary;
mod marshal;
pub mod state;
pub mod state_iterator;

use reifydb_sdk::{
	common::extern_c::wire::callbacks::{builder::BuilderCallbacks, memory::MemoryCallbacks},
	flow::operator::extern_c::wire::callbacks::{
		OperatorCallbacks, dictionary::DictionaryCallbacks, state::StateCallbacks,
	},
};

use crate::{callbacks::extern_c::builder, procedure::callbacks::extern_c::memory};

pub fn create_host_callbacks() -> OperatorCallbacks {
	OperatorCallbacks {
		memory: MemoryCallbacks {
			alloc: memory::host_alloc,
			free: memory::host_free,
		},
		state: StateCallbacks {
			get: state::host_state_get,
			set: state::host_state_set,
			remove: state::host_state_remove,
			clear: state::host_state_clear,
			prefix: state::host_state_prefix,
			range: state::host_state_range,
			iterator_next: state::host_state_iterator_next,
			iterator_free: state::host_state_iterator_free,
			get_many: state::host_state_get_many,
			get_or_create_row_numbers: state::host_get_or_create_row_numbers,
			remove_row_number: state::host_remove_row_number,
			remove_row_numbers_below: state::host_remove_row_numbers_below,
			intern_groups: state::host_intern_groups,
			lookup_groups: state::host_lookup_groups,
			arm_timer: state::host_arm_timer,
			disarm_timer: state::host_disarm_timer,
			flow_watermark: state::host_flow_watermark,
			reclaim_group_identity: state::host_reclaim_group_identity,
		},
		dictionary: DictionaryCallbacks {
			id_by_name: dictionary::host_dictionary_id_by_name,
			find: dictionary::host_dictionary_find,
			get: dictionary::host_dictionary_get,
		},
		builder: BuilderCallbacks {
			acquire: builder::host_builder_acquire,
			data_ptr: builder::host_builder_data_ptr,
			offsets_ptr: builder::host_builder_offsets_ptr,
			bitvec_ptr: builder::host_builder_bitvec_ptr,
			grow: builder::host_builder_grow,
			commit: builder::host_builder_commit,
			release: builder::host_builder_release,
			emit_diff: builder::host_builder_emit_diff,
		},
	}
}
