# Test Each File

[![github](https://img.shields.io/badge/github-8da0cb?style=for-the-badge&labelColor=555555&logo=github)](https://github.com/binary-banter/test-each-file)
&ensp;[![crates-io](https://img.shields.io/badge/crates.io-fc8d62?style=for-the-badge&labelColor=555555&logo=rust)](https://crates.io/crates/test_each_file)
&ensp;[![docs-rs](https://img.shields.io/badge/docs.rs-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs)](https://docs.rs/test_each_file)

Easily generate tests for files in a specified directory for comprehensive testing.

A simple example of the macro is shown below:

```rust
test_each_file! { in "./resources" => test }

fn test(content: &str) {
    // Make assertions on the `content` of the file here.
}
```

Given the following file structure:

```txt
- resources
  - a.txt
  - b.txt
  - extra
    - c.txt
- src
  - main.rs
```

The macro expands to:

```rust
#[test]
fn a() {
    // The macro actually uses an absolute path for `a.txt` behind the scenes
    test(include_str!("../resources/a.txt"))
}

#[test]
fn b() {
    test(include_str!("../resources/b.txt"))
}

mod extra {
    use super::*;

    #[test]
    fn c() {
        test(include_str!("../resources/extra/c.txt"))
    }
}
```

## Generate submodule

The tests can automatically be inserted into a module, by using the `as` keyword. For example:

```rust
test_each_file! { in "./resources" as example => test }
```

This will wrap the tests above in an additional `mod example { ... }`.
This feature is useful when `test_each_file!` is used multiple times in a single file, to prevent that the generated
tests have the same name.

## File grouping

Sometimes it may be preferable to write a test that takes the contents of multiple files as input.
A common use-case for this is testing a function that performs a transformation from a given input (`.in` file) to an
output (`.out` file).

```rust
test_each_file! { for ["in", "out"] in "./resources" => test }

fn test([input, output]: [&str; 2]) {
    // Make assertions on the content of the `input` and `output` files here.
}
```

Both the `.in` and `.out` files must exist and be located in the same directory, as demonstrated below:

```txt
- resources
  - a.in
  - a.out
  - b.in
  - b.out
  - extra
    - c.in
    - c.out
- src
  - main.rs
```

Note that `.in` and `.out` are just examples here - any number of unique extensions can be given of arbitrary types.

## Test each path

A similar macro exists for passing all the paths in a given directory. This macro behaves identically to `test_each_file!`, 
except that it passes the paths of the files rather than the contents. It is usually preferable to use `test_each_file!`,
because it includes the files in the binary, whereas the paths still need to be there during run-time for `test_each_path!`.

```rust
test_each_path! { for ["in", "out"] in "./resources" => test }

fn test([input, output]: [&Path; 2]) {
    // Make assertions on the path of `input` and `output` here.
}
```

## More examples

The expression that is called on each file can also be a closure, for example:

```rust
test_each_file! { in "./resources" => |c: &str| assert!(c.contains("Hello World")) }
```

Multiple attributes can optionally be applied to each test, for example:

```rust
test_each_file! { #[ignore, cfg(target_os = "linux")] in "./resources" => test }
```

You can specify that each test is async, in this case a test macro such as `#[tokio::test]` must be specified explicitly. For example:

```rust
test_each_file! { #[tokio::test] async in "./resources" => test }
```

All the options above can be combined, for example:

```rust
test_each_file! { #[tokio::test, ignore, cfg(target_os = "linux")] async for ["in", "out"] in "./resources" as example => async |[a, b]: [&str; 2]| assert_eq!(a, b) }
```
