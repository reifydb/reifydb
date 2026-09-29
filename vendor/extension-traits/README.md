# `::extension-traits`

#### Annotation to easily define _ad-hoc_ / one-shot extension traits.

[![Repository](https://img.shields.io/badge/repository-GitHub-brightgreen.svg)](
https://github.com/danielhenrymantilla/ext-trait.rs)
[![Latest version](https://img.shields.io/crates/v/extension-traits.svg)](
https://crates.io/crates/extension-traits)
[![Documentation](https://docs.rs/extension-traits/badge.svg)](
https://docs.rs/extension-traits)
[![MSRV](https://img.shields.io/badge/MSRV-1.42.0-white)](
https://gist.github.com/danielhenrymantilla/8e5b721b3929084562f8f65668920c33)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](
https://github.com/rust-secure-code/safety-dance/)
[![License](https://img.shields.io/crates/l/extension-traits.svg)](
https://github.com/danielhenrymantilla/ext-trait.rs/blob/master/LICENSE-ZLIB)
[![CI](https://github.com/danielhenrymantilla/ext-trait.rs/workflows/CI/badge.svg)](
https://github.com/danielhenrymantilla/ext-trait.rs/actions)

### Examples

  - #### `Also`

    ```rust ,no_run
    #[macro_use]
    extern crate extension_traits;

    #[extension(trait Also)]
    impl<T> T {
        fn also (mut self, f: impl FnOnce(&mut Self))
          -> Self
        {
            f(&mut self);
            self
        }
    }

    fn main ()
    {
        use ::std::{collections::HashMap, ops::Not};

        let /* immut */ map = HashMap::with_capacity(2).also(|m| {
            m.insert("foo", 42);
            m.insert("bar", 27);
        });
        assert!(map.contains_key("foo"));
        assert!(map.contains_key("bar"));
        assert!(map.contains_key("baz").not());
    }
    ```

  - #### `WithPath`

    ```rust ,no_run
    #[macro_use]
    extern crate extension_traits;

    use ::std::{error::Error, path::{Path, PathBuf}};

    #[extension(trait WithPath)]
    impl PathBuf {
        fn with (mut self, segment: impl AsRef<Path>)
          -> PathBuf
        {
            self.push(segment);
            self
        }
    }

    fn main ()
      -> Result<(), Box<dyn Error>>
    {
        let some_dir = PathBuf::from(::std::env::var("MY_LIB_SOME_DIR")?);
        // Contrary to chaining `.join()`, this reuses the memory!
        let some_subdir = some_dir.with("some").with("sub").with("dir");
        // …
        Ok(())
    }
    ```

  - #### `Context`

    ```rust ,no_run
    #[macro_use]
    extern crate extension_traits;

    use ::std::{error::Error, fmt::Display};

    #[extension(trait Context)]
    impl<Ok, Err : Display> Result<Ok, Err> {
        fn context (self, prefix: impl Display)
          -> Result<Ok, String>
        {
            self.map_err(|err| format!("{}: {}", prefix, err))
        }
    }

    fn main ()
      -> Result<(), Box<dyn Error>>
    {
        let file_contents =
            ::std::fs::read_to_string("some/file")
                .context("Error when opening some/file")?
        ;
        // …
        Ok(())
    }
    ```

Similar to <https://docs.rs/extension-trait>, but for the following:

### Features

  - Supports generics (see [`Context`](#context))

  - search/`grep 'trait TraitName'`-friendly!
