use nom_locate::LocatedSpan;

#[derive(Debug, Clone, PartialEq)]
pub struct Span {
    /// The offset represents the position of the fragment relatively to
    /// the input of the parser. It starts at offset 0.
    pub offset: Offset,
    /// The line number of the fragment relatively to the input of the
    /// parser. It starts at line 1.
    pub line: Line,

    pub fragment: String,
}

impl<'a> From<LocatedSpan<&'a str>> for Span {
    fn from(value: LocatedSpan<&'a str>) -> Self {
        Self { offset: Offset(value.location_offset()), line: Line(value.location_line()), fragment: value.fragment().to_string() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Offset(pub usize);

impl PartialEq<usize> for Offset {
    fn eq(&self, other: &usize) -> bool {
        self.0 == *other
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Line(pub u32);

impl PartialEq<u32> for Line {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}
