// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Error, Token, TokenKind};
use nom::branch::alt;
use nom::bytes::tag_no_case;
use nom::character::complete::alphanumeric1;
use nom::combinator::{map, not, peek};
use nom::sequence::terminated;
use nom::{IResult, Input, Parser};
use nom_locate::LocatedSpan;
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
    };
}

keyword! {
    Select     => "SELECT",
    By         => "BY",
    From       => "FROM",
    Where      => "WHERE",
    Group      => "GROUP",
    Having     => "HAVING",
    Order      => "ORDER",
    Limit      => "LIMIT",
    Offset     => "OFFSET",

    Join       => "JOIN",
    On         => "ON",
    As         => "AS",
    Using      => "USING",
    Union      => "UNION",
    Intersect  => "INTERSECT",
    Except     => "EXCEPT",

    Insert     => "INSERT",
    Into       => "INTO",
    Values     => "VALUES",
    Update     => "UPDATE",
    Set        => "SET",
    Delete     => "DELETE",

    Let        => "LET",
    If         => "IF",
    Else       => "ELSE",
    End        => "END",
    Loop       => "LOOP",
    Return     => "RETURN",

    Define     => "DEFINE",
    Function   => "FUNCTION",
    Call       => "CALL",

    Describe   => "DESCRIBE",
    Show       => "SHOW",
    Create     => "CREATE",
    Drop       => "DROP",

    And        => "AND",
    Or         => "OR",
    Not        => "NOT",
    In         => "IN",
    Between    => "BETWEEN",
    Like       => "LIKE",
    Is         => "IS",
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

type Span<'a> = LocatedSpan<&'a str>;

fn keyword_tag<'a>(kw: Keyword, tag: &'static str) -> impl Parser<Span<'a>, Output = Keyword, Error = nom::error::Error<Span<'a>>> + 'a {
    move |input: Span<'a>| map(terminated(tag_no_case(tag), not(peek(alphanumeric1))), move |_| kw).parse(input)
}
pub(crate) fn parse_keyword(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let start = input;

    let parser = alt((
        alt((
            keyword_tag(Keyword::Select, "SELECT"),
            keyword_tag(Keyword::By, "BY"),
            keyword_tag(Keyword::From, "FROM"),
            keyword_tag(Keyword::Where, "WHERE"),
            keyword_tag(Keyword::Group, "GROUP"),
            keyword_tag(Keyword::Having, "HAVING"),
            keyword_tag(Keyword::Order, "ORDER"),
            keyword_tag(Keyword::Limit, "LIMIT"),
            keyword_tag(Keyword::Offset, "OFFSET"),
            keyword_tag(Keyword::Insert, "INSERT"),
        )),
        alt((
            keyword_tag(Keyword::Into, "INTO"),
            keyword_tag(Keyword::Values, "VALUES"),
            keyword_tag(Keyword::Update, "UPDATE"),
            keyword_tag(Keyword::Set, "SET"),
            keyword_tag(Keyword::Delete, "DELETE"),
            keyword_tag(Keyword::Join, "JOIN"),
            keyword_tag(Keyword::On, "ON"),
            keyword_tag(Keyword::As, "AS"),
            keyword_tag(Keyword::Using, "USING"),
            keyword_tag(Keyword::Union, "UNION"),
            keyword_tag(Keyword::Intersect, "INTERSECT"),
            keyword_tag(Keyword::Except, "EXCEPT"),
            keyword_tag(Keyword::Let, "LET"),
            keyword_tag(Keyword::If, "IF"),
            keyword_tag(Keyword::Else, "ELSE"),
            keyword_tag(Keyword::End, "END"),
        )),
        alt((
            keyword_tag(Keyword::Loop, "LOOP"),
            keyword_tag(Keyword::Return, "RETURN"),
            keyword_tag(Keyword::Define, "DEFINE"),
            keyword_tag(Keyword::Function, "FUNCTION"),
            keyword_tag(Keyword::Call, "CALL"),
            keyword_tag(Keyword::Describe, "DESCRIBE"),
            keyword_tag(Keyword::Show, "SHOW"),
            keyword_tag(Keyword::Create, "CREATE"),
            keyword_tag(Keyword::Drop, "DROP"),
            keyword_tag(Keyword::And, "AND"),
            keyword_tag(Keyword::Or, "OR"),
            keyword_tag(Keyword::Not, "NOT"),
            keyword_tag(Keyword::In, "IN"),
            keyword_tag(Keyword::Between, "BETWEEN"),
            keyword_tag(Keyword::Like, "LIKE"),
            keyword_tag(Keyword::Is, "IS"),
        )),
    ));

    parser.map(|kw| Token { kind: TokenKind::Keyword(kw), span: start.take(kw.as_str().len()).into() }).parse(input)
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::keyword::{parse_keyword, Keyword};
    use crate::rql::frontend::lex::{LocatedSpan, TokenKind};

    #[test]
    fn test_parse_keyword_invalid() {
        let input = LocatedSpan::new("foobar rest");
        let result = parse_keyword(input);

        assert!(result.is_err(), "expected error parsing invalid keyword, got: {:?}", result);
    }

    fn check_keyword(keyword: Keyword, repr: &str) {
        for mode in [false, true] {
            let input_str = if mode { format!("{repr} rest") } else { format!("{} rest", repr.to_uppercase()) };

            let input = LocatedSpan::new(input_str.as_str());

            let result = parse_keyword(input).unwrap();
            let (remaining, token) = result;

            assert_eq!(TokenKind::Keyword(keyword), token.kind, "kind mismatch for keyword: {}", repr);
            assert_eq!(token.span.fragment, repr);
            assert_eq!(token.span.offset, 0);
            assert_eq!(token.span.line, 1);
            assert_eq!(*remaining.fragment(), " rest");
        }
    }

    macro_rules! generate_keyword_tests {
        ($($name:ident => ($variant:ident, $repr:literal)),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    check_keyword(Keyword::$variant, $repr);
                }
            )*
        };
    }

    generate_keyword_tests! {
        test_keyword_select => (Select, "SELECT"),
        test_keyword_by => (By, "BY"),
        test_keyword_from => (From, "FROM"),
        test_keyword_where => (Where, "WHERE"),
        test_keyword_group => (Group, "GROUP"),
        test_keyword_having => (Having, "HAVING"),
        test_keyword_order => (Order, "ORDER"),
        test_keyword_limit => (Limit, "LIMIT"),
        test_keyword_offset => (Offset, "OFFSET"),
        test_keyword_insert => (Insert, "INSERT"),
        test_keyword_into => (Into, "INTO"),
        test_keyword_values => (Values, "VALUES"),
        test_keyword_update => (Update, "UPDATE"),
        test_keyword_set => (Set, "SET"),
        test_keyword_delete => (Delete, "DELETE"),
        test_keyword_join => (Join, "JOIN"),
        test_keyword_on => (On, "ON"),
        test_keyword_as => (As, "AS"),
        test_keyword_using => (Using, "USING"),
        test_keyword_union => (Union, "UNION"),
        test_keyword_intersect => (Intersect, "INTERSECT"),
        test_keyword_except => (Except, "EXCEPT"),
        test_keyword_let => (Let, "LET"),
        test_keyword_if => (If, "IF"),
        test_keyword_else => (Else, "ELSE"),
        test_keyword_end => (End, "END"),
        test_keyword_loop => (Loop, "LOOP"),
        test_keyword_return => (Return, "RETURN"),
        test_keyword_define => (Define, "DEFINE"),
        test_keyword_function => (Function, "FUNCTION"),
        test_keyword_call => (Call, "CALL"),
        test_keyword_describe => (Describe, "DESCRIBE"),
        test_keyword_show => (Show, "SHOW"),
        test_keyword_create => (Create, "CREATE"),
        test_keyword_drop => (Drop, "DROP"),
        test_keyword_and => (And, "AND"),
        test_keyword_or => (Or, "OR"),
        test_keyword_not => (Not, "NOT"),
        test_keyword_in => (In, "IN"),
        test_keyword_between => (Between, "BETWEEN"),
        test_keyword_like => (Like, "LIKE"),
        test_keyword_is => (Is, "IS"),
    }
}
