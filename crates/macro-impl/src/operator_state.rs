// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Ident, Path, parse_str, parse2};

pub fn operator_state_impl(
	attr: TokenStream,
	item: TokenStream,
	crate_path: &str,
	value_path: &str,
) -> TokenStream {
	let seal = if attr.is_empty() {
		false
	} else {
		match parse2::<Ident>(attr.clone()) {
			Ok(ident) if ident == "seal" => true,
			_ => {
				return Error::new_spanned(
					attr,
					"operator_state accepts no argument or the single marker `seal`",
				)
				.to_compile_error();
			}
		}
	};
	let input: DeriveInput = match parse2(item.clone()) {
		Ok(input) => input,
		Err(err) => return err.to_compile_error(),
	};
	let root: Path = match parse_str(crate_path) {
		Ok(path) => path,
		Err(err) => return err.to_compile_error(),
	};
	let value_root: Path = match parse_str(value_path) {
		Ok(path) => path,
		Err(err) => return err.to_compile_error(),
	};

	let name = &input.ident;
	let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
	let extra_bounds = quote! {
		Self: #root::state::ArchiveState,
	};
	let merged_where = match where_clause {
		Some(existing) => {
			let predicates = &existing.predicates;
			quote! { where #predicates, #extra_bounds }
		}
		None => quote! { where #extra_bounds },
	};

	let seal_impl = if seal {
		quote! {
			#[automatically_derived]
			impl #impl_generics #root::state::SealMutableState for #name #ty_generics #merged_where {}
		}
	} else {
		quote! {}
	};

	quote! {
		#[derive(
			#root::state::archive::Archive,
			#root::state::archive::Serialize,
			#root::state::archive::Deserialize,
		)]
		#[rkyv(crate = #root::state::archive::rkyv)]
		#item

		#seal_impl

		#[automatically_derived]
		impl #impl_generics #root::state::OperatorState for #name #ty_generics #merged_where {
			type Archived = <Self as #root::state::archive::Archive>::Archived;

			fn encode_state(
				&self,
				now: #value_root::value::datetime::DateTime,
			) -> ::core::result::Result<#root::state::StateBytes, #root::state::StateError> {
				#root::state::encode_archive(self, now)
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

			unsafe fn archived_seal_trusted(
				bytes: &mut #root::state::StateBytes,
			) -> #root::state::archive::rkyv::seal::Seal<'_, Self::Archived> {
				// SAFETY: forwarded contract; see OperatorState::archived_seal_trusted.
				unsafe { #root::state::access_archive_seal_trusted::<Self>(bytes) }
			}

			fn materialize(
				archived: &Self::Archived,
			) -> ::core::result::Result<Self, #root::state::StateError> {
				#root::state::materialize_archive::<Self>(archived)
			}
		}
	}
}
