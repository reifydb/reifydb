# error-code

[![Crates.io](https://img.shields.io/crates/v/error-code.svg)](https://crates.io/crates/error-code)
[![Documentation](https://docs.rs/error-code/badge.svg)](https://docs.rs/crate/error-code/)
[![Rust](https://github.com/DoumanAsh/error-code/actions/workflows/rust.yml/badge.svg)](https://github.com/DoumanAsh/error-code/actions/workflows/rust.yml)

Error code library provides generic errno/winapi error wrapper

User can define own `Category` if you want to create new error wrapper.

## Features

- `std` - Enables `std::error::Error` implementation and conversion from `std::io::Error`

## Usage

```rust
use error_code::ErrorCode;

use std::fs::File;

File::open("non_existing");
println!("{}", ErrorCode::last_system());
```
