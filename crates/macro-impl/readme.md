# reifydb-macro-impl

Implementation for ReifyDB macros. **Not for direct use** - use `reifydb-macro`, `reifydb-derive`, or `reifydb-client-derive` instead.

## Why This Crate Exists

Proc-macro crates in Rust can **only** export functions tagged with `#[proc_macro]`, `#[proc_macro_derive]`, or `#[proc_macro_attribute]`. They cannot export regular functions or types.

This creates a problem: we need multiple proc-macro crates (`reifydb-macro`, `reifydb-derive`, `reifydb-client-derive`) that share the same implementation logic but generate code with different crate paths.

The solution is to put all implementation logic in this regular library crate using `proc_macro2` types. The proc-macro crates become thin wrappers that convert between `proc_macro::TokenStream` and `proc_macro2::TokenStream`.

## Architecture

```
                     reifydb-macro-impl
                    (this crate - shared logic)
                    /         |         \
                   /          |          \
        reifydb-macro   reifydb-derive   reifydb-client-derive
        (reifydb_type)     (reifydb)       (reifydb_client)
```

Each wrapper crate is a one-liner:

```rust
#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
    reifydb_macro_impl::derive_from_frame_with_crate(input.into(), "crate_name").into()
}
```

## Adding Support for a New Crate

To add `FromFrame` support for a new crate (e.g., `my-reifydb-wrapper`):

1. Create a new proc-macro crate
2. Add `reifydb-macro-impl` as a dependency
3. Create a thin wrapper:

```rust
use proc_macro::TokenStream;

#[proc_macro_derive(FromFrame, attributes(frame))]
pub fn derive_from_frame(input: TokenStream) -> TokenStream {
    reifydb_macro_impl::derive_from_frame_with_crate(input.into(), "my_reifydb_wrapper").into()
}
```

The generated code will reference types like `::my_reifydb_wrapper::Frame`, `::my_reifydb_wrapper::FromFrame`, etc.
