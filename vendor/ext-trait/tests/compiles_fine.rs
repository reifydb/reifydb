#[macro_use] extern crate ext_trait;

#[test]
fn empty () {
    #[extension(trait Foo)]
    impl () {}

    #[extension(trait Bar)]
    impl<> () {}

    #[extension(trait Baz)]
    impl () where Self : Copy {}

    #[extension(trait Quux)]
    impl<T> () where T : ?Sized {
        type Assoc = T;
    }

    mod scoped {
        #[extension(pub trait Pub)]
        impl () {}

        #[extension(pub(crate) trait PubCrate)]
        impl () {}
    }
    impl dyn scoped::Pub {}
    impl dyn scoped::PubCrate {}
}

#[test]
fn context ()
{
    #[extension(trait Context)]
    impl<Ok, Err : ::core::fmt::Display> Result<Ok, Err> {
        fn context (self, prefix: impl ::core::fmt::Display)
          -> Result<Ok, String>
        {
            self.map_err(|err| format!("{}: {}", prefix, err))
        }
    }
    match ::std::fs::read_to_string("/non existent").context("Test") {
        | Ok(_) => panic!("Managed to open `/non existent`??"),
        | Err(s) => assert!(s.starts_with("Test: ")),
    }
}

#[test]
fn all_the_assocs ()
{
    #[extension(trait Assocs)]
    impl<T> T {
        type AssocTy = Option<T>;
        const FOO: Self::AssocTy = None;
    }

    let _: <() as Assocs<_>>::AssocTy = <()>::FOO;
}
