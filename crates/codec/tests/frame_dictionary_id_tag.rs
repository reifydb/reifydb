// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, format::Encoding, options::EncodeOptions};
use reifydb_value::value::{
	container::dictionary_array::{self, dictionary_array},
	dictionary::{DictionaryEntryId, DictionaryId},
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
};

#[test]
fn a_frame_carries_dictionary_entry_ids_but_never_the_dictionary_id() {
	// The frame wire has no slot for the dictionary id, so a decoded column must say none, never a guessed id.
	let entries = vec![DictionaryEntryId::U4(0), DictionaryEntryId::U4(7), DictionaryEntryId::U4(u32::MAX)];
	for options in [EncodeOptions::default(), EncodeOptions::forced(Encoding::Plain)] {
		let frame = Frame::new(vec![FrameColumn {
			name: "c".to_string(),
			data: FrameColumnData::DictionaryId {
				container: dictionary_array(entries.clone()),
				dictionary_id: Some(DictionaryId(42)),
			},
		}]);
		let bytes = encode_frames(&[frame], &options).expect("a dictionary id column encodes");
		let decoded =
			decode_frames(&bytes).expect("a dictionary id column decodes").remove(0).columns.remove(0).data;
		let FrameColumnData::DictionaryId {
			container,
			dictionary_id,
		} = &decoded
		else {
			panic!("expected a dictionary id column, got {:?}", decoded.get_type());
		};
		assert_eq!(*dictionary_id, None, "the decoded column carries no dictionary id");
		assert_eq!(dictionary_array::iter(container).collect::<Vec<_>>(), entries, "every entry id comes back");
	}
}
