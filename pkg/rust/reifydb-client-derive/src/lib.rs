// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]
// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
// This file is licensed under the Apache-2.0, see license.md file

use proc_macro::TokenStream;
use reifydb_macro_impl::derive_from_frame_with_crate;

#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
	derive_from_frame_with_crate(input.into(), "reifydb_client::value").into()
}
