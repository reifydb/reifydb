// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod fixture;
pub mod from;
pub mod to;
pub mod types;
pub mod wire_type;

pub const NONE_MARKER: &str = "⟪none⟫";

fn marker_parts() -> (&'static str, &'static str) {
	let (close_at, _) = NONE_MARKER.char_indices().last().expect("none marker is not empty");
	NONE_MARKER.split_at(close_at)
}

pub fn none_marker(wrapped: u32) -> String {
	if wrapped == 0 {
		return NONE_MARKER.to_string();
	}
	let (open, close) = marker_parts();
	format!("{open}:{wrapped}{close}")
}

pub fn none_marker_depth(payload: &str) -> Option<u32> {
	if payload == NONE_MARKER {
		return Some(0);
	}
	let (open, close) = marker_parts();
	let wrapped = payload.strip_prefix(open)?.strip_suffix(close)?.strip_prefix(':')?;
	wrapped.parse::<u32>().ok().filter(|k| *k >= 1)
}

pub fn is_none_marker(payload: &str) -> bool {
	none_marker_depth(payload).is_some()
}
