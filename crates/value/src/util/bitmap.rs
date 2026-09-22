// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, result::Result as StdResult};

use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, Buffer, NullBuffer};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

pub fn slice(bits: &BooleanBuffer, start: usize, end: usize) -> BooleanBuffer {
	let end = end.min(bits.len());
	let start = start.min(end);
	bits.slice(start, end - start)
}

pub fn filter(bits: &BooleanBuffer, mask: &BooleanBuffer) -> BooleanBuffer {
	let mut kept = BooleanBufferBuilder::new(mask.count_set_bits());
	for i in mask.set_indices().take_while(|&i| i < bits.len()) {
		kept.append(bits.value(i));
	}
	kept.finish()
}

pub fn reorder(bits: &BooleanBuffer, indices: &[usize]) -> BooleanBuffer {
	BooleanBuffer::collect_bool(indices.len(), |i| indices[i] < bits.len() && bits.value(indices[i]))
}

pub fn resize(bits: &BooleanBuffer, len: usize) -> BooleanBuffer {
	if bits.len() >= len {
		return bits.slice(0, len);
	}
	let mut resized = BooleanBufferBuilder::new(len);
	resized.append_buffer(bits);
	resized.append_n(len - bits.len(), false);
	resized.finish()
}

pub fn assert_nulls_len(nulls: Option<&NullBuffer>, len: usize) {
	if let Some(nulls) = nulls {
		assert_eq!(nulls.len(), len, "validity of {} rows does not match an array of {len} rows", nulls.len());
	}
}

pub fn and_nulls(left: &NullBuffer, right: &NullBuffer) -> NullBuffer {
	assert_eq!(left.len(), right.len(), "validity of {} and {} rows cannot be combined", left.len(), right.len());
	NullBuffer::new(left.inner() & right.inner())
}

pub fn slice_nulls(nulls: Option<&NullBuffer>, start: usize, end: usize) -> Option<NullBuffer> {
	nulls.map(|nulls| NullBuffer::new(slice(nulls.inner(), start, end)))
}

pub fn filter_nulls(nulls: Option<&NullBuffer>, mask: &BooleanBuffer) -> Option<NullBuffer> {
	nulls.map(|nulls| NullBuffer::new(filter(nulls.inner(), mask)))
}

pub fn reorder_nulls(nulls: Option<&NullBuffer>, indices: &[usize]) -> Option<NullBuffer> {
	nulls.map(|nulls| NullBuffer::new(reorder(nulls.inner(), indices)))
}

fn clear_tail(bytes: &mut [u8], len: usize) {
	let used = len % 8;
	if used != 0
		&& let Some(last) = bytes.last_mut()
	{
		*last &= (1u8 << used) - 1;
	}
}

pub fn packed_bytes(bits: &BooleanBuffer) -> Cow<'_, [u8]> {
	let len = bits.len();
	let byte_len = len.div_ceil(8);
	if bits.offset().is_multiple_of(8) {
		let start = bits.offset() / 8;
		let bytes = &bits.values()[start..start + byte_len];
		let used = len % 8;
		if used == 0 || bytes[byte_len - 1] >> used == 0 {
			return Cow::Borrowed(bytes);
		}
	}
	let mut packed = bits.sliced().as_slice()[..byte_len].to_vec();
	clear_tail(&mut packed, len);
	Cow::Owned(packed)
}

#[derive(Serialize)]
#[serde(rename = "BitVecInner")]
struct BitVecInnerRef<'a> {
	bits: &'a [u8],
	len: usize,
}

#[derive(Deserialize)]
struct BitVecInner {
	bits: Vec<u8>,
	len: usize,
}

pub fn serialize<Ser: Serializer>(bits: &BooleanBuffer, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	BitVecInnerRef {
		bits: &packed_bytes(bits),
		len: bits.len(),
	}
	.serialize(serializer)
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<BooleanBuffer, D::Error> {
	let BitVecInner {
		mut bits,
		len,
	} = BitVecInner::deserialize(deserializer)?;
	let byte_len = len.div_ceil(8);
	if bits.len() < byte_len {
		return Err(D::Error::invalid_length(bits.len(), &"at least ceil(len / 8) packed bytes"));
	}
	bits.truncate(byte_len);
	clear_tail(&mut bits, len);
	Ok(BooleanBuffer::new(Buffer::from_vec(bits), 0, len))
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use arrow_buffer::BooleanBuffer;
	use postcard::to_allocvec;
	use serde::{Deserialize, Serialize};
	use serde_json::{from_str, to_string};

	use super::{filter, packed_bytes, reorder, slice};

	#[derive(Serialize, Deserialize)]
	struct Wrap(#[serde(with = "super")] BooleanBuffer);

	fn pattern(len: usize) -> Vec<bool> {
		(0..len).map(|i| i % 3 != 1).collect()
	}

	fn bits_of(bits: &BooleanBuffer) -> Vec<bool> {
		bits.iter().collect()
	}

	#[test]
	fn slice_clamps_out_of_range_bounds() {
		// Arrow's slice(offset, len) panics past the end; ours must clamp end to len like the old bitvec.
		let bits = BooleanBuffer::from(pattern(10));
		let tail = slice(&bits, 4, 100);
		assert_eq!(tail.len(), 6);
		assert_eq!(bits_of(&tail), pattern(10)[4..].to_vec());
		assert_eq!(slice(&bits, 12, 20).len(), 0);
	}

	#[test]
	fn slice_takes_end_not_length_as_its_second_argument() {
		// Reading the second argument as a length would give 5 rows here instead of 3.
		let bits = BooleanBuffer::from(pattern(10));
		let mid = slice(&bits, 2, 5);
		assert_eq!(mid.len(), 3);
		assert_eq!(bits_of(&mid), pattern(10)[2..5].to_vec());
	}

	#[test]
	fn slice_with_start_past_end_is_empty() {
		// start > end must clamp start to end instead of underflowing end - start.
		let bits = BooleanBuffer::from(pattern(10));
		assert_eq!(slice(&bits, 7, 3).len(), 0);
		assert_eq!(slice(&bits, 30, 20).len(), 0);
	}

	#[test]
	fn filter_short_mask_drops_the_tail_and_long_mask_is_ignored_past_len() {
		// A mask shorter than the bits must drop the rows past it; a longer one must not read past len.
		let bits = BooleanBuffer::from(vec![true, false, true, true]);
		let short = filter(&bits, &BooleanBuffer::from(vec![true, true]));
		assert_eq!(bits_of(&short), vec![true, false]);
		let long = filter(&bits, &BooleanBuffer::from(vec![false, true, false, true, true, true]));
		assert_eq!(bits_of(&long), vec![false, true]);
	}

	#[test]
	fn reorder_out_of_range_gives_false() {
		// An out of range index must read as false, as the old containers did, never panic.
		let bits = BooleanBuffer::from(vec![true, true]);
		assert_eq!(bits_of(&reorder(&bits, &[1, 5, 0])), vec![true, false, true]);
	}

	#[test]
	fn packed_bytes_of_a_view_have_a_zero_tail() {
		// A view past bit 0 must repack from bit 0 and zero every bit past len.
		let bits = BooleanBuffer::new_set(16);
		assert_eq!(packed_bytes(&slice(&bits, 3, 9)).as_ref(), &[0b0011_1111]);
		let aligned = slice(&bits, 8, 13);
		assert_eq!(packed_bytes(&aligned).as_ref(), &[0b0001_1111]);
		assert!(matches!(packed_bytes(&aligned), Cow::Owned(_)));
	}

	#[test]
	fn packed_bytes_after_not_have_a_zero_tail() {
		// Arrow's `!` sets the bits past len; a raw byte reader must never see them.
		let bits = BooleanBuffer::from(vec![true, false, true]);
		let inverted = !&bits;
		assert_eq!(packed_bytes(&inverted).as_ref(), &[0b0000_0010]);
	}

	#[test]
	fn packed_bytes_borrow_a_clean_buffer() {
		// A fresh buffer already holds the packed form, so it must not be copied.
		let bits = BooleanBuffer::from(pattern(12));
		let packed = packed_bytes(&bits);
		assert!(matches!(packed, Cow::Borrowed(_)));
		assert_eq!(packed.len(), 2);
	}

	#[test]
	fn serialize_writes_the_bitvec_shape() {
		// Without the {bits, len} shape of packed bytes, stored and wire columns stop decoding.
		let bits = BooleanBuffer::from(vec![true, false, true]);
		assert_eq!(to_string(&Wrap(bits.clone())).unwrap(), r#"{"bits":[5],"len":3}"#);
		assert_eq!(to_allocvec(&Wrap(bits)).unwrap(), vec![1, 5, 3]);
	}

	#[test]
	fn serialize_of_a_view_equals_a_fresh_buffer() {
		// A view at any bit offset must serialize exactly like a fresh buffer of the same bits.
		let all = pattern(20);
		let view = slice(&BooleanBuffer::from(all.clone()), 3, 17);
		let fresh = BooleanBuffer::from(all[3..17].to_vec());
		assert_eq!(to_string(&Wrap(view.clone())).unwrap(), to_string(&Wrap(fresh.clone())).unwrap());
		assert_eq!(to_allocvec(&Wrap(view)).unwrap(), to_allocvec(&Wrap(fresh)).unwrap());
	}

	#[test]
	fn serialize_after_not_has_a_zero_tail() {
		// Arrow's `!` leaves 1 bits past len; they must not reach the serialized bytes.
		let inverted = !&BooleanBuffer::from(vec![true, false, true]);
		assert_eq!(to_string(&Wrap(inverted)).unwrap(), r#"{"bits":[2],"len":3}"#);
	}

	#[test]
	fn deserialize_rejects_fewer_bytes_than_len_needs() {
		// Accepting short bits would panic on the first read past them; it must be a serde error.
		assert!(from_str::<Wrap>(r#"{"bits":[],"len":5}"#).is_err());
		assert!(from_str::<Wrap>(r#"{"bits":[1],"len":9}"#).is_err());
	}

	#[test]
	fn deserialize_cuts_extra_bytes_and_cleans_dirty_bits() {
		// Extra bytes and bits past len must be dropped, or equal columns serialize differently.
		let Wrap(bits) = from_str::<Wrap>(r#"{"bits":[255,7,9],"len":3}"#).unwrap();
		assert_eq!(bits_of(&bits), vec![true, true, true]);
		assert_eq!(bits.inner().len(), 1);
		assert_eq!(bits.values(), &[0b0000_0111]);
		assert_eq!(to_string(&Wrap(bits)).unwrap(), r#"{"bits":[7],"len":3}"#);
	}

	#[test]
	fn deserialize_round_trips_postcard() {
		// A postcard round trip must keep every bit and the length.
		let bits = BooleanBuffer::from(pattern(13));
		let bytes = to_allocvec(&Wrap(bits.clone())).unwrap();
		let Wrap(back) = postcard::from_bytes::<Wrap>(&bytes).unwrap();
		assert_eq!(back, bits);
	}
}
