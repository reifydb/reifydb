// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Path, parse_str, parse2};

pub fn operator_state_impl(attr: TokenStream, item: TokenStream, crate_path: &str, value_path: &str) -> TokenStream {
	if !attr.is_empty() {
		return Error::new_spanned(attr, "operator_state accepts no argument").to_compile_error();
	}
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

	quote! {
		#[derive(
			#root::row::operator::archive::Archive,
			#root::row::operator::archive::Serialize,
			#root::row::operator::archive::Deserialize,
		)]
		#[rkyv(crate = #root::row::operator::archive::rkyv)]
		#item

		#[automatically_derived]
		impl #impl_generics #root::row::operator::OperatorState for #name #ty_generics #merged_where {
			fn encode_state(
				&self,
				now: #value_root::value::datetime::DateTime,
			) -> ::core::result::Result<#root::row::operator::EncodedOperatorRow, #root::row::operator::OperatorError> {
				#root::row::operator::encode_archive(self, now)
			}

			fn decode_state(
				bytes: &#root::row::operator::EncodedOperatorRow,
			) -> ::core::result::Result<Self, #root::row::operator::OperatorError> {
				#root::row::operator::decode_archive::<Self>(bytes)
			}
		}
	}
}
