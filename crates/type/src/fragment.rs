// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{
	cell::RefCell,
	cmp,
	cmp::Ordering,
	collections::HashMap,
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
	sync::Arc,
};

use serde::{
	Deserialize, Serialize,
	de::{self, EnumAccess, MapAccess, VariantAccess, Visitor},
};

const INTERN_CAP: usize = 4096;

thread_local! {
	static INTERN: RefCell<HashMap<Arc<str>, ()>> = RefCell::new(HashMap::new());
}

fn intern(text: &str) -> Arc<str> {
	INTERN.with(|table| {
		let mut guard = table.borrow_mut();
		if let Some((existing, _)) = guard.get_key_value(text) {
			return existing.clone();
		}
		if guard.len() >= INTERN_CAP {
			return Arc::from(text);
		}
		let arc: Arc<str> = Arc::from(text);
		guard.insert(arc.clone(), ());
		arc
	})
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StatementColumn(pub u32);

impl Deref for StatementColumn {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<i32> for StatementColumn {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StatementLine(pub u32);

impl Deref for StatementLine {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<i32> for StatementLine {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

#[derive(Debug, Clone, PartialEq, Hash, Serialize, Default)]
pub enum Fragment {
	#[default]
	None,

	Statement {
		text: Arc<str>,
		line: StatementLine,
		column: StatementColumn,
	},

	Internal {
		text: Arc<str>,
	},
}

enum FragmentVariant {
	None,
	Statement,
	Internal,
}

impl<'de> Deserialize<'de> for FragmentVariant {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		struct VariantVisitor;

		impl<'de> Visitor<'de> for VariantVisitor {
			type Value = FragmentVariant;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("variant identifier")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					0 => Ok(FragmentVariant::None),
					1 => Ok(FragmentVariant::Statement),
					2 => Ok(FragmentVariant::Internal),
					_ => Err(de::Error::invalid_value(
						de::Unexpected::Unsigned(value),
						&"variant index 0 <= i < 3",
					)),
				}
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					"None" => Ok(FragmentVariant::None),
					"Statement" => Ok(FragmentVariant::Statement),
					"Internal" => Ok(FragmentVariant::Internal),
					_ => Err(de::Error::unknown_variant(value, VARIANTS)),
				}
			}

			fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					b"None" => Ok(FragmentVariant::None),
					b"Statement" => Ok(FragmentVariant::Statement),
					b"Internal" => Ok(FragmentVariant::Internal),
					_ => Err(de::Error::unknown_variant(&String::from_utf8_lossy(value), VARIANTS)),
				}
			}
		}

		deserializer.deserialize_identifier(VariantVisitor)
	}
}

enum StatementField {
	Text,
	Line,
	Column,
}

impl<'de> Deserialize<'de> for StatementField {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		struct FieldVisitor;

		impl<'de> Visitor<'de> for FieldVisitor {
			type Value = StatementField;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("field identifier")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					0 => Ok(StatementField::Text),
					1 => Ok(StatementField::Line),
					2 => Ok(StatementField::Column),
					_ => Err(de::Error::invalid_value(
						de::Unexpected::Unsigned(value),
						&"field index 0 <= i < 3",
					)),
				}
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					"text" => Ok(StatementField::Text),
					"line" => Ok(StatementField::Line),
					"column" => Ok(StatementField::Column),
					_ => Err(de::Error::unknown_field(value, STATEMENT_FIELDS)),
				}
			}
		}

		deserializer.deserialize_identifier(FieldVisitor)
	}
}

enum InternalField {
	Text,
}

impl<'de> Deserialize<'de> for InternalField {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		struct FieldVisitor;

		impl<'de> Visitor<'de> for FieldVisitor {
			type Value = InternalField;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("field identifier")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					0 => Ok(InternalField::Text),
					_ => Err(de::Error::invalid_value(
						de::Unexpected::Unsigned(value),
						&"field index 0 <= i < 1",
					)),
				}
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				match value {
					"text" => Ok(InternalField::Text),
					_ => Err(de::Error::unknown_field(value, INTERNAL_FIELDS)),
				}
			}
		}

		deserializer.deserialize_identifier(FieldVisitor)
	}
}

struct InternedText(Arc<str>);

impl<'de> Deserialize<'de> for InternedText {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		struct InternedVisitor;

		impl<'de> Visitor<'de> for InternedVisitor {
			type Value = InternedText;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("a string")
			}

			fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(InternedText(intern(value)))
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(InternedText(intern(value)))
			}

			fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(InternedText(intern(&value)))
			}
		}

		deserializer.deserialize_str(InternedVisitor)
	}
}

const VARIANTS: &[&str] = &["None", "Statement", "Internal"];
const STATEMENT_FIELDS: &[&str] = &["text", "line", "column"];
const INTERNAL_FIELDS: &[&str] = &["text"];

impl<'de> Deserialize<'de> for Fragment {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		struct FragmentVisitor;

		impl<'de> Visitor<'de> for FragmentVisitor {
			type Value = Fragment;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("enum Fragment")
			}

			fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
			where
				A: EnumAccess<'de>,
			{
				let (variant, access) = data.variant::<FragmentVariant>()?;
				match variant {
					FragmentVariant::None => {
						access.unit_variant()?;
						Ok(Fragment::None)
					}
					FragmentVariant::Statement => {
						access.struct_variant(STATEMENT_FIELDS, StatementVisitor)
					}
					FragmentVariant::Internal => {
						access.struct_variant(INTERNAL_FIELDS, InternalVisitor)
					}
				}
			}
		}

		struct StatementVisitor;

		impl<'de> Visitor<'de> for StatementVisitor {
			type Value = Fragment;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("struct variant Fragment::Statement")
			}

			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: de::SeqAccess<'de>,
			{
				let text: Arc<str> =
					seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
				let line: StatementLine =
					seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
				let column: StatementColumn =
					seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
				Ok(Fragment::Statement {
					text,
					line,
					column,
				})
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: MapAccess<'de>,
			{
				let mut text: Option<Arc<str>> = None;
				let mut line: Option<StatementLine> = None;
				let mut column: Option<StatementColumn> = None;
				while let Some(key) = map.next_key::<StatementField>()? {
					match key {
						StatementField::Text => {
							if text.is_some() {
								return Err(de::Error::duplicate_field("text"));
							}
							text = Some(map.next_value()?);
						}
						StatementField::Line => {
							if line.is_some() {
								return Err(de::Error::duplicate_field("line"));
							}
							line = Some(map.next_value()?);
						}
						StatementField::Column => {
							if column.is_some() {
								return Err(de::Error::duplicate_field("column"));
							}
							column = Some(map.next_value()?);
						}
					}
				}
				Ok(Fragment::Statement {
					text: text.ok_or_else(|| de::Error::missing_field("text"))?,
					line: line.ok_or_else(|| de::Error::missing_field("line"))?,
					column: column.ok_or_else(|| de::Error::missing_field("column"))?,
				})
			}
		}

		struct InternalVisitor;

		impl<'de> Visitor<'de> for InternalVisitor {
			type Value = Fragment;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("struct variant Fragment::Internal")
			}

			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: de::SeqAccess<'de>,
			{
				let text: InternedText =
					seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
				Ok(Fragment::Internal {
					text: text.0,
				})
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: MapAccess<'de>,
			{
				let mut text: Option<Arc<str>> = None;
				while let Some(key) = map.next_key::<InternalField>()? {
					match key {
						InternalField::Text => {
							if text.is_some() {
								return Err(de::Error::duplicate_field("text"));
							}
							let value: InternedText = map.next_value()?;
							text = Some(value.0);
						}
					}
				}
				Ok(Fragment::Internal {
					text: text.ok_or_else(|| de::Error::missing_field("text"))?,
				})
			}
		}

		deserializer.deserialize_enum("Fragment", VARIANTS, FragmentVisitor)
	}
}

impl Fragment {
	pub fn text(&self) -> &str {
		match self {
			Fragment::None => "",
			Fragment::Statement {
				text,
				..
			}
			| Fragment::Internal {
				text,
				..
			} => text,
		}
	}

	pub fn line(&self) -> StatementLine {
		match self {
			Fragment::Statement {
				line,
				..
			} => *line,
			_ => StatementLine(1),
		}
	}

	pub fn column(&self) -> StatementColumn {
		match self {
			Fragment::Statement {
				column,
				..
			} => *column,
			_ => StatementColumn(0),
		}
	}

	pub fn sub_fragment(&self, offset: usize, length: usize) -> Fragment {
		let text = self.text();
		let end = cmp::min(offset + length, text.len());
		let sub_text = if offset < text.len() {
			&text[offset..end]
		} else {
			""
		};

		match self {
			Fragment::None => Fragment::None,
			Fragment::Statement {
				line,
				column,
				..
			} => Fragment::Statement {
				text: Arc::from(sub_text),
				line: *line,
				column: StatementColumn(column.0 + offset as u32),
			},
			Fragment::Internal {
				..
			} => Fragment::Internal {
				text: Arc::from(sub_text),
			},
		}
	}

	pub fn with_text(&self, text: impl AsRef<str>) -> Fragment {
		let text = Arc::from(text.as_ref());
		match self {
			Fragment::Statement {
				line,
				column,
				..
			} => Fragment::Statement {
				text,
				line: *line,
				column: *column,
			},
			Fragment::Internal {
				..
			} => Fragment::Internal {
				text,
			},
			Fragment::None => Fragment::Internal {
				text,
			},
		}
	}
}

impl Fragment {
	pub fn internal(text: impl AsRef<str>) -> Self {
		Fragment::Internal {
			text: intern(text.as_ref()),
		}
	}

	pub fn testing(text: impl AsRef<str>) -> Self {
		Fragment::Statement {
			text: Arc::from(text.as_ref()),
			line: StatementLine(1),
			column: StatementColumn(0),
		}
	}

	pub fn testing_empty() -> Self {
		Self::testing("")
	}

	pub fn merge_all(fragments: impl IntoIterator<Item = Fragment>) -> Fragment {
		let mut fragments: Vec<Fragment> = fragments.into_iter().collect();
		assert!(!fragments.is_empty());

		fragments.sort();

		let first = fragments.first().unwrap();

		let mut text = String::with_capacity(fragments.iter().map(|f| f.text().len()).sum());
		for fragment in &fragments {
			text.push_str(fragment.text());
		}

		match first {
			Fragment::None => Fragment::None,
			Fragment::Statement {
				line,
				column,
				..
			} => Fragment::Statement {
				text: Arc::from(text),
				line: *line,
				column: *column,
			},
			Fragment::Internal {
				..
			} => Fragment::Internal {
				text: Arc::from(text),
			},
		}
	}

	pub fn fragment(&self) -> &str {
		self.text()
	}
}

impl AsRef<str> for Fragment {
	fn as_ref(&self) -> &str {
		self.text()
	}
}

impl Display for Fragment {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(self.text(), f)
	}
}

impl PartialOrd for Fragment {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Fragment {
	fn cmp(&self, other: &Self) -> Ordering {
		self.column().cmp(&other.column()).then(self.line().cmp(&other.line()))
	}
}

impl Eq for Fragment {}

impl From<String> for Fragment {
	fn from(s: String) -> Self {
		Fragment::Internal {
			text: Arc::from(s),
		}
	}
}

impl From<&str> for Fragment {
	fn from(s: &str) -> Self {
		Fragment::Internal {
			text: Arc::from(s),
		}
	}
}

impl Fragment {
	pub fn statement(text: impl AsRef<str>, line: u32, column: u32) -> Self {
		Fragment::Statement {
			text: Arc::from(text.as_ref()),
			line: StatementLine(line),
			column: StatementColumn(column),
		}
	}

	pub fn none() -> Self {
		Fragment::None
	}
}

impl PartialEq<str> for Fragment {
	fn eq(&self, other: &str) -> bool {
		self.text() == other
	}
}

impl PartialEq<&str> for Fragment {
	fn eq(&self, other: &&str) -> bool {
		self.text() == *other
	}
}

impl PartialEq<String> for Fragment {
	fn eq(&self, other: &String) -> bool {
		self.text() == other.as_str()
	}
}

impl PartialEq<String> for &Fragment {
	fn eq(&self, other: &String) -> bool {
		self.text() == other.as_str()
	}
}

pub trait LazyFragment {
	fn fragment(&self) -> Fragment;
}

impl<F> LazyFragment for F
where
	F: Fn() -> Fragment,
{
	fn fragment(&self) -> Fragment {
		self()
	}
}

impl LazyFragment for &Fragment {
	fn fragment(&self) -> Fragment {
		(*self).clone()
	}
}

impl LazyFragment for Fragment {
	fn fragment(&self) -> Fragment {
		self.clone()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use postcard::{from_bytes, to_allocvec};

	use super::*;

	fn internal_text(fragment: &Fragment) -> &Arc<str> {
		match fragment {
			Fragment::Internal {
				text,
			} => text,
			other => panic!("expected Internal fragment, got {other:?}"),
		}
	}

	fn statement_text(fragment: &Fragment) -> &Arc<str> {
		match fragment {
			Fragment::Statement {
				text,
				..
			} => text,
			other => panic!("expected Statement fragment, got {other:?}"),
		}
	}

	#[test]
	fn two_internal_constructions_share_storage() {
		// The whole point of interning: distinct calls for the same column name
		// must reuse one heap allocation, otherwise the hotspot is not fixed.
		let a = Fragment::internal("price_share_test_a");
		let b = Fragment::internal("price_share_test_a");
		assert!(Arc::ptr_eq(internal_text(&a), internal_text(&b)));
	}

	#[test]
	fn deserialize_internal_shares_storage_with_construction() {
		// Deserialize is the actual 1.8M/s hotspot; a deserialized Internal text
		// must land on the same shared Arc as a constructed one, proving no fresh
		// per-diff allocation.
		let constructed = Fragment::internal("vwap_share_test");
		let bytes = to_allocvec(&constructed).unwrap();
		let decoded: Fragment = from_bytes(&bytes).unwrap();
		assert_eq!(constructed, decoded);
		assert!(Arc::ptr_eq(internal_text(&constructed), internal_text(&decoded)));
	}

	#[test]
	fn statement_text_is_not_interned() {
		// Statement text is arbitrary RQL (unbounded cardinality); interning it
		// would leak. Two Statements with identical text must NOT share storage,
		// and they must stay out of the intern table entirely.
		let a = Fragment::statement("select arbitrary rql text", 1, 0);
		let b = Fragment::statement("select arbitrary rql text", 1, 0);
		assert!(!Arc::ptr_eq(statement_text(&a), statement_text(&b)));

		let bytes = to_allocvec(&a).unwrap();
		let decoded_one: Fragment = from_bytes(&bytes).unwrap();
		let decoded_two: Fragment = from_bytes(&bytes).unwrap();
		assert!(!Arc::ptr_eq(statement_text(&decoded_one), statement_text(&decoded_two)));
	}

	#[test]
	fn round_trip_preserves_value_for_each_variant() {
		// Custom Deserialize must remain byte-compatible with the derived Serialize
		// for every variant, including the field-bearing ones.
		let variants = [
			Fragment::None,
			Fragment::statement("from foo map { a }", 7, 3),
			Fragment::internal("round_trip_internal"),
		];
		for variant in variants {
			let bytes = to_allocvec(&variant).unwrap();
			let decoded: Fragment = from_bytes(&bytes).unwrap();
			assert_eq!(variant, decoded);
		}
	}

	#[test]
	fn round_trip_preserves_statement_line_and_column() {
		// A seq-shaped Statement decode must place text/line/column in the right
		// slots; a transposition would still be value-equal on text alone.
		let original = Fragment::statement("xy", 42, 99);
		let bytes = to_allocvec(&original).unwrap();
		let decoded: Fragment = from_bytes(&bytes).unwrap();
		assert_eq!(decoded.line(), StatementLine(42));
		assert_eq!(decoded.column(), StatementColumn(99));
		assert_eq!(decoded.text(), "xy");
	}
}
