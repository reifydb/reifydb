// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct SHA1_CTX {
    pub state: [u32; 5],
    pub count: [u32; 2],
    pub buffer: [u8; 64],
}

unsafe extern "C" {
    pub fn SHA1Init(context: *mut SHA1_CTX);
    pub fn SHA1Update(context: *mut SHA1_CTX, data: *const u8, len: u32);
    pub fn SHA1Final(digest: *mut u8, context: *mut SHA1_CTX);
    pub fn SHA1(hash_out: *mut u8, str: *const u8, len: u32);
}
