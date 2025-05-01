// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::Error;
use std::fmt::{Display, Formatter};

macro_rules! keyword {
    (
        $( $variant:ident => $string:literal ),* $(,)?
    ) => {

        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub enum Keyword {
            $( $variant ),*
        }

        impl Keyword {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $( Keyword::$variant => $string ),*
                }
            }
        }

        impl TryFrom<&str> for Keyword {
            type Error = Error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                debug_assert!(value.chars().all(|c| c.is_uppercase()), "keyword must be uppercase");
                match value {
                    $( $string => Ok(Keyword::$variant) ),*,
                    _ => Err(Error("not a keyword".to_string()))
                }
            }
        }

        const ALL_KEYWORDS: &[Keyword] = &[
            $( Keyword::$variant ),*
        ];
    };
}

keyword! {
    As => "AS",
    From => "FROM",
    Insert => "INSERT",
    Into => "INTO",
    Select => "SELECT",
    Values => "VALUES",
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::keyword::{Keyword, ALL_KEYWORDS};
    use crate::rql::frontend::lex::Error;

    #[test]
    fn test_valid_keywords() {
        for keyword in ALL_KEYWORDS {
            let k = keyword.as_str();
            let parsed = Keyword::try_from(k).unwrap();
            assert_eq!(parsed, *keyword, "unable to parse valid keyword {}", k);
        }
    }

    #[test]
    fn test_invalid_keyword() {
        let err = Keyword::try_from("UNKNOWN").unwrap_err();
        assert_eq!(err, Error("not a keyword".to_string()));
    }

    #[test]
    #[should_panic(expected = "keyword must be uppercase")]
    fn test_lowercase_keyword_panics() {
        let _ = Keyword::try_from("select");
    }
}
