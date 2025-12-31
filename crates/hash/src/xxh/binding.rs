// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct Hash128 {
    pub low: u64,
    pub high: u64}


unsafe extern "C" {
    pub fn XXH32(input: *const u8, length: usize, seed: u32) -> u32;
    pub fn XXH64(input: *const u8, length: usize, seed: u64) -> u64;
    pub fn XXH3_64bits(data: *const u8, len: usize) -> u64;
    pub fn XXH3_128bits(data: *const u8, len: usize) -> Hash128;
}
