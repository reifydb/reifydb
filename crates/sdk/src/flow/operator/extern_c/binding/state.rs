// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, ptr, ptr::null_mut, slice::from_raw_parts};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{key::operator_state::GroupId, state::store::TimerKind};
use reifydb_flow::operator::state::reclaim::ReclaimOutcome;
use reifydb_value::{
	count::Count,
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};
use tracing::{Span, instrument};

use crate::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		key_ref::ExternCKeyRef,
		status::{EXTERN_C_END_OF_ITERATION, EXTERN_C_NOT_FOUND, EXTERN_C_OK},
	},
	error::{Result, SdkError},
	flow::operator::extern_c::{
		binding::context::ExternCContext,
		wire::{
			callbacks::state::GROUP_ABSENT,
			iterators::ExternCStateIterator,
			state::{ExternCStateEntry, ExternCStateSlice},
		},
	},
};

#[instrument(name = "flow::operator::state::extern_c:get", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	found
))]
pub(crate) fn get(ctx: &ExternCContext, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
	let key_bytes = key.as_bytes();
	let mut output = ExternCBuffer {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; key_bytes outlives the callback, which only reads it. On EXTERN_C_OK the host
	// writes a buffer of output.len initialised bytes, copied out before memory.free releases it with the length
	// it was allocated at.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			&mut output,
		);

		if result == EXTERN_C_OK {
			if output.ptr.is_null() || output.len == 0 {
				Span::current().record("found", false);
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();

				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				Span::current().record("found", true);
				Ok(Some(EncodedBytes(CowVec::new(value_bytes))))
			}
		} else if result == EXTERN_C_NOT_FOUND {
			Span::current().record("found", false);
			Ok(None)
		} else {
			Err(SdkError::Other(format!("host_state_get failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::extern_c:set", level = "trace", skip(ctx, value), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	value_len = value.as_ref().len()
))]
pub(crate) fn set(ctx: &mut ExternCContext, key: &EncodedKey, value: &EncodedBytes) -> Result<()> {
	let key_bytes = key.as_bytes();
	let value_bytes = value.as_ref();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; key_bytes and value_bytes borrow guest allocations that outlive the callback,
	// which only reads them.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.set)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			value_bytes.as_ptr(),
			value_bytes.len(),
		);

		if result == EXTERN_C_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_state_set failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::extern_c::binding::state::remove", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len()
))]
pub(crate) fn remove(ctx: &mut ExternCContext, key: &EncodedKey) -> Result<()> {
	let key_bytes = key.as_bytes();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; key_bytes outlives the callback, which only reads it.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
		);

		if result == EXTERN_C_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_state_remove failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::extern_c:get_many", level = "debug", skip(ctx, keys), fields(
	operator_id = ctx.operator_id().0,
	key_count = keys.len(),
	result_count
))]
pub(crate) fn get_many(ctx: &ExternCContext, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	if keys.is_empty() {
		Span::current().record("result_count", 0);
		return Ok(Vec::new());
	}

	let key_refs: Vec<ExternCKeyRef> = keys
		.iter()
		.map(|key| {
			let bytes = key.as_bytes();
			ExternCKeyRef {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
			}
		})
		.collect();

	let mut iterator: *mut ExternCStateIterator = null_mut();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; key_refs borrows keys that outlive the callback. The handle the host opens is
	// passed once to collect_iterator_results, discharging its precondition that the handle is fresh and freed
	// exactly there.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get_many)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_refs.as_ptr(),
			key_refs.len(),
			&mut iterator,
		);

		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_state_get_many failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

#[instrument(name = "flow::operator::state::extern_c:prefix", level = "debug", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	prefix_len = prefix.as_bytes().len(),
	result_count
))]
pub(crate) fn prefix(ctx: &ExternCContext, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	let prefix_bytes = prefix.as_bytes();
	let mut iterator: *mut ExternCStateIterator = null_mut();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; prefix_bytes outlives the callback. The handle the host opens is passed once to
	// collect_iterator_results, discharging its precondition that the handle is fresh and freed exactly there.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.prefix)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			prefix_bytes.as_ptr(),
			prefix_bytes.len(),
			&mut iterator,
		);

		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_state_prefix failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[instrument(name = "flow::operator::extern_c::binding::state::range", level = "debug", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	result_count
))]
pub(crate) fn range(
	ctx: &ExternCContext,
	start: Bound<&EncodedKey>,
	end: Bound<&EncodedKey>,
) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	let mut iterator: *mut ExternCStateIterator = null_mut();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; each bound pointer is null with length 0 or borrows a key that outlives the
	// callback. The handle the host opens is passed once to collect_iterator_results, which owns and frees it.
	unsafe {
		let (start_ptr, start_len, start_bound_type) = match start {
			Bound::Unbounded => (ptr::null(), 0, BOUND_UNBOUNDED),
			Bound::Included(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_INCLUDED),
			Bound::Excluded(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_EXCLUDED),
		};

		let (end_ptr, end_len, end_bound_type) = match end {
			Bound::Unbounded => (ptr::null(), 0, BOUND_UNBOUNDED),
			Bound::Included(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_INCLUDED),
			Bound::Excluded(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_EXCLUDED),
		};

		let result = ((*ctx.ctx).callbacks.state.range)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			start_ptr,
			start_len,
			start_bound_type,
			end_ptr,
			end_len,
			end_bound_type,
			&mut iterator,
		);

		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_state_range failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

/// # Safety
///
/// `ctx.ctx` must point to a live `ExternCContextRaw` whose state callbacks are valid,
/// and `iterator` must be null or an open handle from that context's state
/// scan/range callback that has not yet been freed. This takes ownership of the
/// handle and frees it before returning, so the caller must not free it again.
unsafe fn collect_iterator_results(
	ctx: &ExternCContext,
	iterator: *mut ExternCStateIterator,
) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	if iterator.is_null() {
		Span::current().record("result_count", 0);
		return Ok(Vec::new());
	}

	const ITERATOR_BATCH_CAP: usize = 256;
	let empty = ExternCStateSlice {
		ptr: ptr::null(),
		len: 0,
	};
	let mut batch = [ExternCStateEntry {
		key: empty,
		value: empty,
	}; ITERATOR_BATCH_CAP];
	let mut results = Vec::new();

	loop {
		let mut out_len = 0usize;
		// SAFETY: iterator was checked non-null above and is still the open handle this call owns; batch is a
		// live array of ITERATOR_BATCH_CAP entries and out_len a local slot, so the host writes at most
		// ITERATOR_BATCH_CAP entries in bounds.
		let next_result = unsafe {
			((*ctx.ctx).callbacks.state.iterator_next)(
				iterator,
				batch.as_mut_ptr(),
				ITERATOR_BATCH_CAP,
				&mut out_len,
			)
		};

		if next_result != EXTERN_C_OK && next_result != EXTERN_C_END_OF_ITERATION {
			// SAFETY: iterator is the open handle this call owns and has not been freed yet; the return
			// immediately after means it is freed exactly once.
			unsafe { ((*ctx.ctx).callbacks.state.iterator_free)(iterator) };
			return Err(SdkError::Other(format!(
				"host_state_iterator_next failed with code {}",
				next_result
			)));
		}

		for entry in batch.iter().take(out_len) {
			if entry.key.ptr.is_null() || entry.key.len == 0 {
				continue;
			}
			// SAFETY: the host fills out_len entries whose key ptr/len describe initialised bytes owned
			// by the iterator and valid until the next iterator_next or iterator_free, both of which
			// happen after this copy. Null and zero-length keys are skipped above.
			let key_bytes = unsafe { from_raw_parts(entry.key.ptr, entry.key.len) }.to_vec();
			let value = if !entry.value.ptr.is_null() && entry.value.len > 0 {
				// SAFETY: same iterator-owned lifetime contract as the key slice.
				let value_bytes = unsafe { from_raw_parts(entry.value.ptr, entry.value.len) }.to_vec();
				EncodedBytes(CowVec::new(value_bytes))
			} else {
				EncodedBytes(CowVec::new(Vec::new()))
			};
			results.push((EncodedKey::new(key_bytes), value));
		}

		if next_result == EXTERN_C_END_OF_ITERATION {
			break;
		}
	}

	// SAFETY: iterator is the open handle this call owns; the loop only breaks on the path that has not freed it,
	// so this frees it exactly once.
	unsafe { ((*ctx.ctx).callbacks.state.iterator_free)(iterator) };
	Span::current().record("result_count", results.len());
	Ok(results)
}

#[instrument(name = "flow::operator::extern_c::binding::state::clear", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0
))]
pub(crate) fn clear(ctx: &mut ExternCContext) -> Result<()> {
	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; no guest pointer crosses the boundary here.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.clear)((*ctx.ctx).operator_id, ctx.ctx);

		if result == EXTERN_C_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_state_clear failed with code {}", result)))
		}
	}
}

fn key_refs(keys: &[EncodedKey]) -> Vec<ExternCKeyRef> {
	keys.iter()
		.map(|key| {
			let bytes = key.as_bytes();
			ExternCKeyRef {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
			}
		})
		.collect()
}

pub(crate) fn intern_groups(ctx: &mut ExternCContext, groups: &[EncodedKey]) -> Result<Vec<GroupId>> {
	if groups.is_empty() {
		return Ok(Vec::new());
	}
	let refs = key_refs(groups);
	let mut ids = vec![0u64; groups.len()];

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; refs borrows groups for the duration of the call, and ids is a live, initialised
	// array of exactly groups.len() u64 slots for the host to fill.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.intern_groups)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			refs.as_ptr(),
			refs.len(),
			ids.as_mut_ptr(),
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_intern_groups failed with code {}", result)));
		}
	}

	Ok(ids.into_iter().map(GroupId).collect())
}

pub(crate) fn arm_timer(ctx: &mut ExternCContext, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
	let bytes = key.as_bytes();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; bytes outlives the callback, which only reads it.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.arm_timer)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			due.to_bits(),
			kind as u8,
			bytes.as_ptr(),
			bytes.len(),
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_arm_timer failed with code {}", result)));
		}
	}

	Ok(())
}

pub(crate) fn reclaim_group_identity(ctx: &mut ExternCContext, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
	let mut removed = 0usize;
	let mut more = 0u8;

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null, and the host keeps the ExternCContextRaw alive
	// and aligned for the whole guest call; removed and more are local stack slots.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.reclaim_group_identity)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			group.0,
			limit,
			&mut removed,
			&mut more,
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!(
				"host_reclaim_group_identity failed with code {}",
				result
			)));
		}
	}

	Ok(ReclaimOutcome {
		removed: Count::new(removed as u64),
		more: more != 0,
	})
}

pub(crate) fn flow_watermark(ctx: &mut ExternCContext) -> Result<Option<DateTime>> {
	let mut bits = 0u64;
	let mut present = 0u8;

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null, and the host keeps the ExternCContextRaw alive
	// and aligned for the whole guest call; bits and present are local stack slots.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.flow_watermark)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			&mut bits,
			&mut present,
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_flow_watermark failed with code {}", result)));
		}
	}

	Ok((present != 0).then(|| DateTime::from_bits(bits)))
}

pub(crate) fn disarm_timer(ctx: &mut ExternCContext, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
	let bytes = key.as_bytes();

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; bytes outlives the callback, which only reads it.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.disarm_timer)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			due.to_bits(),
			kind as u8,
			bytes.as_ptr(),
			bytes.len(),
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_disarm_timer failed with code {}", result)));
		}
	}

	Ok(())
}

pub(crate) fn lookup_groups(ctx: &mut ExternCContext, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
	if groups.is_empty() {
		return Ok(Vec::new());
	}
	let refs = key_refs(groups);
	let mut ids = vec![0u64; groups.len()];

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; refs borrows groups for the duration of the call, and ids is a live, initialised
	// array of exactly groups.len() u64 slots for the host to fill.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.lookup_groups)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			refs.as_ptr(),
			refs.len(),
			ids.as_mut_ptr(),
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_lookup_groups failed with code {}", result)));
		}
	}

	Ok(ids.into_iter().map(|id| (id != GROUP_ABSENT).then_some(GroupId(id))).collect())
}

pub(crate) fn get_or_create_row_numbers(
	ctx: &mut ExternCContext,
	group: GroupId,
	keys: &[EncodedKey],
) -> Result<Vec<(RowNumber, bool)>> {
	if keys.is_empty() {
		return Ok(Vec::new());
	}
	let key_refs = key_refs(keys);
	let mut row_numbers = vec![0u64; keys.len()];
	let mut is_new = vec![0u8; keys.len()];

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; key_refs borrows keys for the duration of the call, and row_numbers and is_new are
	// live, initialised arrays of exactly keys.len() slots each for the host to fill.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get_or_create_row_numbers)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			group.0,
			key_refs.as_ptr(),
			key_refs.len(),
			row_numbers.as_mut_ptr(),
			is_new.as_mut_ptr(),
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!(
				"host_get_or_create_row_numbers failed with code {}",
				result
			)));
		}
	}

	Ok(row_numbers.into_iter().zip(is_new).map(|(rn, new)| (RowNumber(rn), new != 0)).collect())
}

pub(crate) fn remove_row_number(ctx: &mut ExternCContext, group: GroupId, key: &EncodedKey) -> Result<()> {
	let key_bytes = key.as_bytes();
	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; key_bytes outlives the callback, which only reads it.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove_row_number)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			group.0,
			key_bytes.as_ptr(),
			key_bytes.len(),
		);
		if result == EXTERN_C_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_remove_row_number failed with code {}", result)))
		}
	}
}

pub(crate) fn remove_row_numbers_below(
	ctx: &mut ExternCContext,
	group: GroupId,
	upper: &EncodedKey,
) -> Result<Vec<RowNumber>> {
	let upper_bytes = upper.as_bytes();
	let mut output = ExternCBuffer {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};
	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; upper_bytes outlives the callback. On EXTERN_C_OK the host writes a buffer of
	// output.len initialised bytes, read before memory.free releases it with the length it was allocated with.
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove_row_numbers_below)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			group.0,
			upper_bytes.as_ptr(),
			upper_bytes.len(),
			&mut output,
		);
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!(
				"host_remove_row_numbers_below failed with code {}",
				result
			)));
		}
		if output.ptr.is_null() || output.len == 0 {
			return Ok(Vec::new());
		}
		let bytes = from_raw_parts(output.ptr, output.len);
		let dropped = bytes
			.chunks_exact(8)
			.map(|chunk| RowNumber(u64::from_le_bytes(chunk.try_into().unwrap())))
			.collect();
		((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
		Ok(dropped)
	}
}
