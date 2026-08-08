// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Ident, Path, parse_str, parse2};

pub fn operator_state_impl(attr: TokenStream, item: TokenStream, crate_path: &str, value_path: &str) -> TokenStream {
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
		Self: #root::row::operator::ArchiveState,
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
			impl #impl_generics #root::row::operator::SealMutableState for #name #ty_generics #merged_where {}
		}
	} else {
		quote! {}
	};

	quote! {
		#[derive(
			#root::row::operator::archive::Archive,
			#root::row::operator::archive::Serialize,
			#root::row::operator::archive::Deserialize,
		)]
		#[rkyv(crate = #root::row::operator::archive::rkyv)]
		#item

		#seal_impl

		#[automatically_derived]
		impl #impl_generics #root::row::operator::OperatorState for #name #ty_generics #merged_where {
			type Archived = <Self as #root::row::operator::archive::Archive>::Archived;

			fn encode_state(
				&self,
				now: #value_root::value::datetime::DateTime,
			) -> ::core::result::Result<#root::row::operator::EncodedOperatorRow, #root::row::operator::OperatorError> {
				#root::row::operator::encode_archive(self, now)
			}

			fn archived(
				bytes: &#root::row::operator::EncodedOperatorRow,
			) -> ::core::result::Result<&Self::Archived, #root::row::operator::OperatorError> {
				#root::row::operator::access_archive::<Self>(bytes)
			}

			unsafe fn archived_trusted(bytes: &#root::row::operator::EncodedOperatorRow) -> &Self::Archived {
				// SAFETY: forwarded contract; see OperatorState::archived_trusted.
				unsafe { #root::row::operator::access_archive_trusted::<Self>(bytes) }
			}

			unsafe fn archived_seal_trusted(
				bytes: &mut #root::row::operator::EncodedOperatorRow,
			) -> #root::row::operator::archive::rkyv::seal::Seal<'_, Self::Archived> {
				// SAFETY: forwarded contract; see OperatorState::archived_seal_trusted.
				unsafe { #root::row::operator::access_archive_seal_trusted::<Self>(bytes) }
			}

			fn materialize(
				archived: &Self::Archived,
			) -> ::core::result::Result<Self, #root::row::operator::OperatorError> {
				#root::row::operator::materialize_archive::<Self>(archived)
			}
		}
	}
}
