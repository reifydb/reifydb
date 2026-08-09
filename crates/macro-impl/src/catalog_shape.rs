// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
	Ident, Result, Token, Type, Visibility, braced,
	parse::{Parse, ParseStream},
	parse2,
	punctuated::Punctuated,
};

enum FieldKind {
	Row(Type),
	Utf8,
	Blob,
	Any,
}

struct Field {
	name: Ident,
	kind: FieldKind,
	optional: bool,
}

impl Parse for Field {
	fn parse(input: ParseStream) -> Result<Self> {
		let name: Ident = input.call(Ident::parse_any)?;
		input.parse::<Token![:]>()?;
		let ty: Type = input.parse()?;
		let optional = input.parse::<Option<Token![?]>>()?.is_some();
		let kind = match marker_of(&ty).as_deref() {
			Some("utf8") => FieldKind::Utf8,
			Some("blob") => FieldKind::Blob,
			Some("any") => FieldKind::Any,
			_ => FieldKind::Row(ty),
		};
		Ok(Field {
			name,
			kind,
			optional,
		})
	}
}

fn marker_of(ty: &Type) -> Option<String> {
	match ty {
		Type::Path(p) if p.qself.is_none() && p.path.segments.len() == 1 => {
			Some(p.path.segments[0].ident.to_string())
		}
		_ => None,
	}
}

struct Shape {
	vis: Visibility,
	name: Ident,
	fields: Vec<Field>,
}

impl Parse for Shape {
	fn parse(input: ParseStream) -> Result<Self> {
		let vis: Visibility = input.parse()?;
		let name: Ident = input.parse()?;
		let body;
		braced!(body in input);
		let fields = Punctuated::<Field, Token![,]>::parse_terminated(&body)?;
		Ok(Shape {
			vis,
			name,
			fields: fields.into_iter().collect(),
		})
	}
}

struct ShapeSet {
	shapes: Vec<Shape>,
}

impl Parse for ShapeSet {
	fn parse(input: ParseStream) -> Result<Self> {
		let mut shapes = Vec::new();
		while !input.is_empty() {
			shapes.push(input.parse()?);
		}
		Ok(ShapeSet {
			shapes,
		})
	}
}

use syn::ext::IdentExt;

fn wire_name(ident: &Ident) -> String {
	ident.unraw().to_string()
}

pub fn catalog_shape(input: TokenStream) -> TokenStream {
	let set: ShapeSet = match parse2(input) {
		Ok(s) => s,
		Err(e) => return e.to_compile_error(),
	};

	let modules = set.shapes.iter().map(|shape| {
		let vis = &shape.vis;
		let name = &shape.name;

		let declarations = shape.fields.iter().map(|f| {
			let wire = wire_name(&f.name);
			match &f.kind {
				FieldKind::Row(ty) => quote! {
					::reifydb_codec::row::shape::RowShapeField::unconstrained(
						#wire,
						<#ty as ::reifydb_value::encoding::RowField>::VALUE_TYPE,
					)
				},
				FieldKind::Utf8 => quote! {
					::reifydb_codec::row::shape::RowShapeField::unconstrained(
						#wire,
						::reifydb_value::value::value_type::ValueType::Utf8,
					)
				},
				FieldKind::Blob => quote! {
					::reifydb_codec::row::shape::RowShapeField::unconstrained(
						#wire,
						::reifydb_value::value::value_type::ValueType::Blob,
					)
				},
				FieldKind::Any => quote! {
					::reifydb_codec::row::shape::RowShapeField::unconstrained(
						#wire,
						::reifydb_value::value::value_type::ValueType::Any,
					)
				},
			}
		});

		let indices = shape.fields.iter().enumerate().map(|(index, f)| {
			let konst = format_ident!("{}", wire_name(&f.name).to_uppercase());
			quote! { #vis const #konst: usize = #index; }
		});

		let accessors = shape.fields.iter().enumerate().map(|(index, f)| {
			let base = wire_name(&f.name);
			let getter = format_ident!("get_{}", base);
			let setter = format_ident!("set_{}", base);
			let try_getter = format_ident!("try_get_{}", base);
			let none_setter = format_ident!("set_{}_none", base);

			let core = match &f.kind {
				FieldKind::Row(ty) => quote! {
					#vis fn #getter(row: &::reifydb_codec::row::catalog::EncodedCatalogRow) -> #ty {
						SHAPE.get::<#ty>(row.as_slice(), #index)
					}
					#vis fn #setter(row: &mut ::reifydb_codec::row::catalog::EncodedCatalogRowBuilder, value: #ty) {
						SHAPE.set::<#ty>(row.builder_mut(), #index, value)
					}
				},
				FieldKind::Utf8 => quote! {
					#vis fn #getter(row: &::reifydb_codec::row::catalog::EncodedCatalogRow) -> &str {
						SHAPE.get_utf8(row.as_slice(), #index)
					}
					#vis fn #setter(
						row: &mut ::reifydb_codec::row::catalog::EncodedCatalogRowBuilder,
						value: impl AsRef<str>,
					) {
						SHAPE.set_utf8(row.builder_mut(), #index, value)
					}
				},
				FieldKind::Blob => quote! {
					#vis fn #getter(row: &::reifydb_codec::row::catalog::EncodedCatalogRow) -> ::reifydb_value::value::blob::Blob {
						SHAPE.get_blob(row.as_slice(), #index)
					}
					#vis fn #setter(
						row: &mut ::reifydb_codec::row::catalog::EncodedCatalogRowBuilder,
						value: &::reifydb_value::value::blob::Blob,
					) {
						SHAPE.set_blob(row.builder_mut(), #index, value)
					}
				},
				FieldKind::Any => quote! {
					#vis fn #getter(row: &::reifydb_codec::row::catalog::EncodedCatalogRow) -> ::reifydb_value::value::Value {
						SHAPE.get_value(row.as_slice(), #index)
					}
					#vis fn #setter(
						row: &mut ::reifydb_codec::row::catalog::EncodedCatalogRowBuilder,
						value: &::reifydb_value::value::Value,
					) {
						SHAPE.set_value(row.builder_mut(), #index, value)
					}
				},
			};

			if !f.optional {
				return core;
			}

			let optional_reads = match &f.kind {
				FieldKind::Row(ty) => quote! {
					#vis fn #try_getter(row: &::reifydb_codec::row::catalog::EncodedCatalogRow) -> Option<#ty> {
						SHAPE.try_get::<#ty>(row.as_slice(), #index)
					}
				},
				FieldKind::Utf8 => quote! {
					#vis fn #try_getter(row: &::reifydb_codec::row::catalog::EncodedCatalogRow) -> Option<&str> {
						SHAPE.try_get_utf8(row.as_slice(), #index)
					}
				},
				FieldKind::Blob | FieldKind::Any => quote! {},
			};

			quote! {
				#core
				#optional_reads
				#vis fn #none_setter(row: &mut ::reifydb_codec::row::catalog::EncodedCatalogRowBuilder) {
					SHAPE.set_none(row.builder_mut(), #index)
				}
			}
		});

		quote! {
			#vis mod #name {
				#[allow(unused_imports)]
				use super::*;
				use ::once_cell::sync::Lazy;
				use ::reifydb_codec::row::shape::{RowFamily, RowShape};

				#vis static SHAPE: Lazy<RowShape> = Lazy::new(|| {
					RowShape::new(RowFamily::Catalog, vec![#(#declarations),*])
				});

				#(#indices)*

				#vis fn allocate() -> ::reifydb_codec::row::catalog::EncodedCatalogRowBuilder {
					SHAPE.allocate_catalog()
				}

				#(#accessors)*
			}
		}
	});

	quote! { #(#modules)* }
}
