//! Crate not intended for direct use.
//! Use https:://docs.rs/ext-trait instead.
#![allow(nonstandard_style, unused_imports)]

use ::core::{
    ops::Not as _,
};
use ::proc_macro::{
    TokenStream,
};
use ::proc_macro2::{
    Span,
    TokenStream as TokenStream2,
    TokenTree as TT,
};
use ::quote::{
    format_ident,
    quote,
    quote_spanned,
    ToTokens,
};
use ::syn::{*,
    parse::{Parse, Parser, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
    Result, // Explicitly shadow it
};

///
#[proc_macro_attribute] pub
fn extension (
    attrs: TokenStream,
    input: TokenStream,
) -> TokenStream
{
    extension_impl(attrs.into(), input.into())
    //  .map(|ret| { println!("{}", ret); ret })
        .unwrap_or_else(|err| {
            let mut errors =
                err .into_iter()
                    .map(|err| Error::new(
                        err.span(),
                        format_args!("`#[extension(trait …)]`: {}", err),
                    ))
            ;
            let mut err = errors.next().unwrap();
            errors.for_each(|cur| err.combine(cur));
            err.to_compile_error()
        })
        .into()
}

struct Attrs {
    pub_: Visibility,
    trait_: Token![trait],
    TraitName: Ident,
}

impl Parse for Attrs {
    fn parse (input: ParseStream<'_>)
      -> Result<Attrs>
    {
        Ok(Self {
            pub_: input.parse()?,
            trait_: input.parse()?,
            TraitName: input.parse()?,
        })
    }
}

/// Example
#[cfg(any())]
const _: () = {
    use ::ext_trait::extension;

    #[extension(trait disregard_err)]
    impl<T, E> Result<T, E> {
        fn disregard_err(self) -> Option<T> { self }
    }
};

fn extension_impl (
    attrs: TokenStream2,
    input: TokenStream2,
) -> Result<TokenStream2>
{
    let trait_def_span = attrs.span();
    let Attrs { pub_, trait_, TraitName } = parse2(attrs)?;
    let ref mut item_impl: ItemImpl = parse2(input)?;
    let (intro_generics, fwd_generics, where_clause) = item_impl.generics.split_for_impl();
    match Option::replace(
        &mut item_impl.trait_,
        (None, parse_quote!( #TraitName #fwd_generics ), <_>::default()),
    )
    {
        | Some((_, _, extraneous_for)) => return Err(Error::new_spanned(
            extraneous_for,
            "expected inherent `impl<…> Type<…>` syntax",
        )),
        | _ => {},
    }
    let ref item_impl = item_impl;
    let each_entry = item_impl.items.iter().map(|it| Ok(match it {
        // We don't deny `pub_` and `default_` annotations *directly*:
        // instead, we forward their extraneous presence to the `trait`
        // definition, so as to trigger an grammar error from the following
        // rust parser pass, which ought to yield a way nicer error message.
        | ImplItem::Const(ImplItemConst {
            vis: pub_,
            defaultness: default_,
            const_token: const_,
            ident: CONST_NAME @ _,
            ty: Ty @ _,
            ..
        }) => quote!(
            #pub_
            #default_
            #const_ #CONST_NAME: #Ty;
        ),

        | ImplItem::Method(ImplItemMethod {
            vis: pub_,
            defaultness: default_,
            sig,
            ..
        }) => {
            let mut sig = sig.clone();
            sig.inputs.iter_mut().for_each(|fn_arg| match fn_arg {
                | FnArg::Receiver(Receiver { reference, mutability, .. }) => {
                    if reference.is_none() {
                        *mutability = None;
                    }
                },
                | FnArg::Typed(PatType { pat, .. }) => {
                    *pat = parse_quote!( _ );
                },
            });
            quote!(
                #pub_
                #default_
                #sig;
            )
        },

        | ImplItem::Type(ImplItemType {
            vis: pub_,
            defaultness: default_,
            type_token: type_,
            ident: TypeName @ _,
            generics,
            semi_token: SEMICOLON @ _,
            ..
        }) => quote! (
            #pub_
            #default_
            #type_ #TypeName #generics
            :
                ?::ext_trait::__::core::marker::Sized
            #SEMICOLON
        ),

        | _ => return Err(Error::new_spanned(it, "unsupported `impl` entry")),
    })).collect::<Result<Vec<_>>>()?;
    Ok(quote_spanned!(trait_def_span=>
        #[allow(nonstandard_style)]
        #pub_
        #trait_ #TraitName #intro_generics
        #where_clause
        {
            #(#each_entry)*
        }

        #item_impl
    ))
}
