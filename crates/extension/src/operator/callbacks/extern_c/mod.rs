// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod context;
pub mod dictionary;
mod marshal;
mod pass_through;
pub mod state;
pub mod state_iterator;

use reifydb_sdk::{
	common::extern_c::wire::callbacks::{builder::BuilderCallbacks, memory::MemoryCallbacks},
	flow::operator::extern_c::wire::callbacks::{OperatorCallbacks, state::StateCallbacks},
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
			remove_many: state::host_state_remove_many,
			range: state::host_state_range,
			sweep_many: state::host_state_sweep_many,
			iterator_next: state::host_state_iterator_next,
			iterator_free: state::host_state_iterator_free,
			get_many: state::host_state_get_many,
			batch_read: state::host_state_batch_read,
			batch_write: state::host_state_batch_write,
			get_or_create_row_numbers: state::host_get_or_create_row_numbers,
			get_or_create_row_numbers_for_groups: state::host_get_or_create_row_numbers_for_groups,
			get_row_numbers: state::host_get_row_numbers,
			remove_row_number: state::host_remove_row_number,
			remove_row_numbers: state::host_remove_row_numbers,
			remove_row_numbers_for_groups: state::host_remove_row_numbers_for_groups,
			arm_timer: state::host_arm_timer,
			disarm_timer: state::host_disarm_timer,
			flow_watermark: state::host_flow_watermark,
			reclaim_group_identity: state::host_reclaim_group_identity,
		},
		dictionary_get_many: dictionary::host_dictionary_get_many,
		pass_through: pass_through::host_pass_through,
		dictionary_find_many: dictionary::host_dictionary_find_many,
		dictionary_intern_many: dictionary::host_dictionary_intern_many,
		builder_bind_dictionary: builder::host_builder_bind_dictionary,
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
