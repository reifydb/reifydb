// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Path, parse_str, parse2};

pub fn operator_state_impl(item: TokenStream, crate_path: &str) -> TokenStream {
	let input: DeriveInput = match parse2(item.clone()) {
		Ok(input) => input,
		Err(err) => return err.to_compile_error(),
	};
	let root: Path = match parse_str(crate_path) {
		Ok(path) => path,
		Err(err) => return err.to_compile_error(),
	};

	let name = &input.ident;
	let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
	let extra_bounds = quote! {
		Self: ::core::marker::Send + 'static,
		Self: for<'a> ::rkyv::Serialize<
			::rkyv::api::high::HighSerializer<
				::rkyv::util::AlignedVec,
				::rkyv::ser::allocator::ArenaHandle<'a>,
				::rkyv::rancor::Error,
			>,
		>,
		Self: ::rkyv::Archive,
		<Self as ::rkyv::Archive>::Archived: ::rkyv::Portable
			+ for<'a> ::rkyv::bytecheck::CheckBytes<
				::rkyv::api::high::HighValidator<'a, ::rkyv::rancor::Error>,
			>
			+ ::rkyv::Deserialize<
				Self,
				::rkyv::rancor::Strategy<::rkyv::de::Pool, ::rkyv::rancor::Error>,
			>,
	};
	let merged_where = match where_clause {
		Some(existing) => {
			let predicates = &existing.predicates;
			quote! { where #predicates, #extra_bounds }
		}
		None => quote! { where #extra_bounds },
	};

	quote! {
		#[derive(::rkyv::Archive, ::rkyv::Serialize, ::rkyv::Deserialize)]
		#item

		#[automatically_derived]
		impl #impl_generics #root::state::OperatorState for #name #ty_generics #merged_where {
			type Archived = <Self as ::rkyv::Archive>::Archived;

			fn encode_state(
				&self,
				now_nanos: u64,
			) -> ::core::result::Result<#root::state::StateBytes, #root::state::StateError> {
				#root::state::encode_archive(self, now_nanos)
			}

			fn archived(
				bytes: &#root::state::StateBytes,
			) -> ::core::result::Result<&Self::Archived, #root::state::StateError> {
				#root::state::access_archive::<Self>(bytes)
			}

			unsafe fn archived_trusted(bytes: &#root::state::StateBytes) -> &Self::Archived {
				// SAFETY: the caller upholds OperatorState::archived_trusted's

				unsafe { #root::state::access_archive_trusted::<Self>(bytes) }
			}

			fn materialize(
				archived: &Self::Archived,
			) -> ::core::result::Result<Self, #root::state::StateError> {
				#root::state::materialize_archive::<Self>(archived)
			}
		}
	}
}
