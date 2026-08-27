// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use proc_macro::TokenStream;
use reifydb_macro_impl::{derive_from_frame_with_crate, derive_heap_size as derive_heap_size_impl};

#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input.into(), "reifydb::value").into()
}

#[proc_macro_derive(HeapSize)]
pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	derive_heap_size_impl(input.into()).into()
}
