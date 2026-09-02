// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use proc_macro::TokenStream;
use reifydb_macro_impl::{
	derive_from_frame_with_crate, derive_heap_size as derive_heap_size_impl, derive_key as derive_key_impl,
	derive_typed_key as derive_typed_key_impl, expand_catalog_shape, operator_state_with_crate,
};

#[proc_macro]
pub fn catalog_shape(input: TokenStream) -> TokenStream {
	expand_catalog_shape(input.into()).into()
}

#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input.into(), "reifydb_value").into()
}

#[proc_macro_derive(HeapSize)]
pub fn derive_heap_size(input: TokenStream) -> TokenStream {
	derive_heap_size_impl(input.into()).into()
}

#[proc_macro_derive(TypedKey)]
pub fn derive_typed_key(input: TokenStream) -> TokenStream {
	derive_typed_key_impl(input.into()).into()
}

#[proc_macro_derive(Key, attributes(key))]
pub fn derive_key(input: TokenStream) -> TokenStream {
	derive_key_impl(input.into()).into()
}

#[proc_macro_attribute]
pub fn operator_state(attr: TokenStream, item: TokenStream) -> TokenStream {
	operator_state_with_crate(attr.into(), item.into(), "::reifydb_codec").into()
}

#[proc_macro_attribute]
pub fn operator_state_facade(attr: TokenStream, item: TokenStream) -> TokenStream {
	operator_state_with_crate(attr.into(), item.into(), "::reifydb::codec").into()
}
