# endian-type

[![crates.io](https://img.shields.io/crates/v/endian-type?label=latest)](https://crates.io/crates/endian-type)
[![Documentation](https://docs.rs/endian-type/badge.svg)](https://docs.rs/endian-type/)
[![License](https://img.shields.io/crates/l/endian-type.svg)]()

Type safe wrappers for types with a defined byte order.

### Example

```rust
use endian_type::{types, BigEndian, LittleEndian, NetworkOrder};

// The endianness reflects the type-safety of the declaration:
let foo = 0xbeef_u32;
let foo_be = BigEndian::from(foo);
assert_eq!(foo_be.to_bytes(), foo.to_be_bytes());
assert_eq!(foo_be.as_byte_slice(), &foo.to_be_bytes());

// One can convert back to the native representation using the `From`/`Into` traits:
assert_eq!(foo, foo_be.into());

// To operate on the wrapped types as if they are regular numbers, one has to
// be explicit and switch between the byte representations:
// Note: Internally, these are just [transmutations](https://doc.rust-lang.org/core/mem/fn.transmute.html) and should not affect performance.
let foo = u128::MAX;
let foo_le = LittleEndian::from(foo);
assert_eq!(
    LittleEndian::<u128>::from_bytes(u128::from_ne_bytes(foo_le.to_bytes()).wrapping_add(1).to_ne_bytes()),
    LittleEndian::<u128>::from(0)
);

// We also have a couple of aliases to be used as helper.
//
// This will assert our `NetworkOrder` type is in accordance with the IETF RFC1700.
let foo = -0xdead_i32;
let foo_no = NetworkOrder::from(foo);
let foo_be = types::i32_be::from(foo);
assert_eq!(foo.to_be_bytes(), foo_no.to_bytes());
assert_eq!(foo.to_be_bytes(), foo_be.to_bytes());
```
