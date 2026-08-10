// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Error, Fields, Path, Type, parse_str, parse2};

fn field_types(data: &Data) -> Vec<&Type> {
	let fields: Vec<&Fields> = match data {
		Data::Struct(item) => vec![&item.fields],
		Data::Enum(item) => item.variants.iter().map(|variant| &variant.fields).collect(),
		Data::Union(item) => return item.fields.named.iter().map(|field| &field.ty).collect(),
	};
	let mut seen = Vec::new();
	let mut types = Vec::new();
	for field in fields.into_iter().flat_map(|fields| fields.iter()) {
		let rendered = field.ty.to_token_stream().to_string();
		if !seen.contains(&rendered) {
			seen.push(rendered);
			types.push(&field.ty);
		}
	}
	types
}

fn field_bounds(data: &Data, trait_path: &str) -> String {
	field_types(data)
		.into_iter()
		.map(|ty| format!("{}: {trait_path}", ty.to_token_stream()))
		.collect::<Vec<_>>()
		.join(", ")
}

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
	let serde_crate = format!("{crate_path}::row::operator::derive::serde");
	let serialize_bound = field_bounds(&input.data, &format!("{serde_crate}::Serialize"));
	let deserialize_bound = field_bounds(&input.data, &format!("{serde_crate}::de::DeserializeOwned"));
	let extra_bounds = quote! {
		Self: #root::row::operator::StateCodec,
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
			#root::row::operator::derive::Serialize,
			#root::row::operator::derive::Deserialize,
		)]
		#[serde(crate = #serde_crate)]
		#[serde(bound(serialize = #serialize_bound, deserialize = #deserialize_bound))]
		#item

		#[automatically_derived]
		impl #impl_generics #root::row::operator::OperatorState for #name #ty_generics #merged_where {
			fn encode_state(
				&self,
				now: #value_root::value::datetime::DateTime,
			) -> ::core::result::Result<#root::row::operator::EncodedOperatorRow, #root::row::operator::OperatorError> {
				#root::row::operator::encode(self, now)
			}

			fn decode_state(
				bytes: &#root::row::operator::EncodedOperatorRow,
			) -> ::core::result::Result<Self, #root::row::operator::OperatorError> {
				#root::row::operator::decode_body::<Self>(bytes)
			}
		}
	}
}
