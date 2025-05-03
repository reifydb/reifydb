// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Error, Span, Token, TokenKind};
use nom::branch::alt;
use nom::bytes::tag_no_case;
use nom::combinator::value;
use nom::{IResult, Input, Parser};
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

    True       => "TRUE",
    False      => "FALSE",
    Null       => "NULL",
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

pub(crate) fn parse_keyword(input: Span) -> IResult<Span, Token> {
    let start = input;

    let parser = alt((
        alt((
            value(Keyword::Select, tag_no_case("SELECT")),
            value(Keyword::By, tag_no_case("By")),
            value(Keyword::From, tag_no_case("FROM")),
            value(Keyword::Where, tag_no_case("WHERE")),
            value(Keyword::Group, tag_no_case("GROUP")),
            value(Keyword::Having, tag_no_case("HAVING")),
            value(Keyword::Order, tag_no_case("ORDER")),
            value(Keyword::Limit, tag_no_case("LIMIT")),
            value(Keyword::Offset, tag_no_case("OFFSET")),
            value(Keyword::Insert, tag_no_case("INSERT")),
            value(Keyword::Into, tag_no_case("INTO")),
            value(Keyword::Values, tag_no_case("VALUES")),
            value(Keyword::Update, tag_no_case("UPDATE")),
            value(Keyword::Set, tag_no_case("SET")),
        )),
        alt((
            value(Keyword::Delete, tag_no_case("DELETE")),
            value(Keyword::Join, tag_no_case("JOIN")),
            value(Keyword::On, tag_no_case("ON")),
            value(Keyword::As, tag_no_case("AS")),
            value(Keyword::Using, tag_no_case("USING")),
            value(Keyword::Union, tag_no_case("UNION")),
            value(Keyword::Intersect, tag_no_case("INTERSECT")),
            value(Keyword::Except, tag_no_case("EXCEPT")),
            value(Keyword::Let, tag_no_case("LET")),
            value(Keyword::If, tag_no_case("IF")),
            value(Keyword::Else, tag_no_case("ELSE")),
            value(Keyword::End, tag_no_case("END")),
            value(Keyword::Loop, tag_no_case("LOOP")),
        )),
        alt((
            value(Keyword::Return, tag_no_case("RETURN")),
            value(Keyword::Define, tag_no_case("DEFINE")),
            value(Keyword::Function, tag_no_case("FUNCTION")),
            value(Keyword::Call, tag_no_case("CALL")),
            value(Keyword::Describe, tag_no_case("DESCRIBE")),
            value(Keyword::Show, tag_no_case("SHOW")),
            value(Keyword::Create, tag_no_case("CREATE")),
            value(Keyword::Drop, tag_no_case("DROP")),
            value(Keyword::True, tag_no_case("TRUE")),
            value(Keyword::False, tag_no_case("FALSE")),
            value(Keyword::Null, tag_no_case("NULL")),
            value(Keyword::And, tag_no_case("AND")),
            value(Keyword::Or, tag_no_case("OR")),
            value(Keyword::Not, tag_no_case("NOT")),
            value(Keyword::In, tag_no_case("IN")),
            value(Keyword::Between, tag_no_case("BETWEEN")),
            value(Keyword::Like, tag_no_case("LIKE")),
            value(Keyword::Is, tag_no_case("IS")),
        )),
    ));

    parser.map(|kw| Token { kind: TokenKind::Keyword(kw), span: start.take(kw.as_str().len()) }).parse(input)
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::keyword::{parse_keyword, Keyword};
    use crate::rql::frontend::lex::{Span, Token, TokenKind};

    #[test]
    fn test_parse_keyword_invalid() {
        let input = Span::new("foobar rest");
        let result = parse_keyword(input);

        assert!(result.is_err(), "expected error parsing invalid keyword, got: {:?}", result);
    }

    fn check_keyword(keyword: Keyword, repr: &str) {
        for mode in [false, true] {
            let input_str = if mode { format!("{repr} rest") } else { format!("{} rest", repr.to_uppercase()) };

            let input = Span::new(&input_str);

            let result = parse_keyword(input).unwrap();
            let (remaining, token) = result;

            let expected = Token { kind: TokenKind::Keyword(keyword), span: Span::new(repr) };

            assert_eq!(token.kind, expected.kind, "kind mismatch for keyword: {}", repr);
            assert_eq!(token.span.fragment(), &repr);
            assert_eq!(token.span.location_offset(), 0);
            assert_eq!(token.span.location_line(), 1);
            assert_eq!(remaining.fragment(), &format!(" rest"));
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
        test_keyword_true => (True, "TRUE"),
        test_keyword_false => (False, "FALSE"),
        test_keyword_null => (Null, "NULL"),
        test_keyword_and => (And, "AND"),
        test_keyword_or => (Or, "OR"),
        test_keyword_not => (Not, "NOT"),
        test_keyword_in => (In, "IN"),
        test_keyword_between => (Between, "BETWEEN"),
        test_keyword_like => (Like, "LIKE"),
        test_keyword_is => (Is, "IS"),
    }
}
