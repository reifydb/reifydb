# The following snippets fail to compile

### Cannot provide an already existing `Trait` in `impl Trait for Ty`

```rust ,compile_fail
use ::ext_trait::*;

#[extension(trait Foo)]
impl<T> Foo for Option<T> {}
```

### Assoc items should not carry visibility attributes

```rust ,compile_fail
use ::ext_trait::*;

#[extension(trait Foo)]
impl<T> Option<T> {
    pub
    unsafe
    fn unwrap_unchecked (self: Self)
      -> T
    {
        self.unwrap_or_else(|| {
            ::core::hint::unreachable_unchecked()
        })
    }
}
```

### … nor `default` attributes

```rust ,compile_fail
#![cfg_attr(all(), allow(incomplete_features), feature(specialization))]
use ::ext_trait::*;

#[extension(trait Foo)]
impl<T> Option<T> {
    default
    unsafe
    fn unwrap_unchecked (self: Self)
      -> T
    {
        self.unwrap_or_else(|| {
            ::core::hint::unreachable_unchecked()
        })
    }
}
```

### Private `#[extension(trait)]` is private.

```rust ,compile_fail
mod scoped {
    use ::ext_trait::*;

#[extension(trait Foo)]
    impl () {}
}
use scoped::Foo;
```
