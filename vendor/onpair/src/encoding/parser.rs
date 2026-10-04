// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The trained encoder: pairs a [`CompactDictionary`] with a [`LongestPrefixMatcher`]
//! that drives encoding. Build with [`Parser::train`]; encode with
//! [`Parser::parse`].

use crate::column::Column;
use crate::core::dictionary::CompactDictionary;
use crate::core::offset::Offset;
use crate::core::types::Token;
use crate::encoding::config::{Config, Error, TrainingConfig};
use crate::encoding::lpm::LongestPrefixMatcher;
use crate::encoding::rows::{ArrowRows, Rows};
use crate::encoding::trainer::{TrainResult, train};

/// A trained encoder. Holds the [`CompactDictionary`] (cloned into each
/// [`Column`] so columns are self-contained) and a crate-private, encode-side
/// longest-prefix matcher built from it.
#[derive(Debug, Clone)]
pub struct Parser {
    /// The trained dictionary: sorted and read-padded.
    pub dict: CompactDictionary,
    pub(crate) lpm: LongestPrefixMatcher,
}

impl Parser {
    /// Train a dictionary against `bytes` / `offsets` and build the matching
    /// matcher. `offsets` has length `n + 1`.
    ///
    /// # Errors
    /// [`Error::InvalidArg`] if `offsets` is empty or its last entry exceeds
    /// `bytes.len()`.
    pub fn train<O: Offset>(bytes: &[u8], offsets: &[O], cfg: Config) -> Result<Self, Error> {
        validate_offsets(bytes, offsets)?;
        Ok(Self::train_unchecked(bytes, offsets, cfg))
    }

    /// Like [`Parser::train`] but skips offset validation. The caller guarantees
    /// `(bytes, offsets)` is a valid Arrow pair (non-empty, monotonic
    /// non-decreasing offsets, last `<= bytes.len()`).
    pub(crate) fn train_unchecked<O: Offset>(bytes: &[u8], offsets: &[O], cfg: Config) -> Self {
        Self::train_rows(&ArrowRows::new(bytes, offsets), cfg)
    }

    /// Train directly from a [`Rows`] input without flattening it first.
    pub fn train_rows<R: Rows + ?Sized>(rows: &R, cfg: Config) -> Self {
        let internal_cfg: TrainingConfig = cfg.into();
        let TrainResult { dict, lpm } = train(rows, &internal_cfg);
        // `train` returns a dictionary that is sorted and read-padded by
        // construction — nothing left to do here.
        Self { dict, lpm }
    }

    /// Encode `bytes` / `offsets` using this parser. The dictionary is cloned
    /// into the returned [`Column`], so the column is self-contained — the
    /// strings need not be the corpus the parser was trained on.
    ///
    /// # Errors
    /// [`Error::InvalidArg`] if `offsets` is empty or its last entry exceeds
    /// `bytes.len()`.
    pub fn parse<O: Offset>(&self, bytes: &[u8], offsets: &[O]) -> Result<Column<O>, Error> {
        validate_offsets(bytes, offsets)?;
        Ok(self.parse_unchecked(bytes, offsets))
    }

    /// Like [`Parser::parse`] but skips offset validation; same caller
    /// guarantees as [`Parser::train_unchecked`].
    pub(crate) fn parse_unchecked<O: Offset>(&self, bytes: &[u8], offsets: &[O]) -> Column<O> {
        let mut codes = Vec::new();
        let mut row_offsets = Vec::new();
        self.parse_rows_into(
            &ArrowRows::new(bytes, offsets),
            &mut codes,
            &mut row_offsets,
        );
        // `self.dict` is already read-padded, so the cloned column dictionary is
        // too.
        Column {
            dict: self.dict.clone(),
            codes,
            row_offsets,
        }
    }

    /// Clear and encode rows into reusable code and offset buffers.
    /// `O` must be wide enough to address the resulting code stream.
    pub fn parse_rows_into<R: Rows + ?Sized, O: Offset>(
        &self,
        rows: &R,
        codes: &mut Vec<Token>,
        row_offsets: &mut Vec<O>,
    ) {
        let n = rows.num_rows();
        codes.clear();
        row_offsets.clear();
        row_offsets.reserve(n + 1);
        row_offsets.push(O::from_usize(0));

        for i in 0..n {
            let row = rows.row(i);
            let mut pos = 0;
            while pos < row.len() {
                let (tok, mlen) = self.lpm.find_longest_match(&row[pos..]);
                codes.push(tok);
                pos += mlen;
            }
            row_offsets.push(O::from_usize(codes.len()));
        }
    }

    /// Encode any [`Rows`] input into a self-contained [`Column`].
    pub fn parse_rows<R: Rows + ?Sized, O: Offset>(&self, rows: &R) -> Column<O> {
        let mut codes = Vec::new();
        let mut row_offsets = Vec::new();
        self.parse_rows_into(rows, &mut codes, &mut row_offsets);
        Column {
            dict: self.dict.clone(),
            codes,
            row_offsets,
        }
    }
}

/// Validate the `(bytes, offsets)` Arrow pair: `offsets` must be non-empty and
/// monotonic non-decreasing (the Arrow contract, debug-asserted), and its last
/// (maximum) offset must fit and be `<= bytes.len()`. `O(1)` in release.
pub(crate) fn validate_offsets<O: Offset>(bytes: &[u8], offsets: &[O]) -> Result<(), Error> {
    debug_assert!(
        offsets
            .windows(2)
            .all(|w| w[0].to_usize() <= w[1].to_usize()),
        "offsets must be monotonic non-decreasing",
    );
    let last = offsets.last().ok_or(Error::InvalidArg)?;
    if last.to_usize() > bytes.len() {
        return Err(Error::InvalidArg);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dictionary::{
        CompactDictionary, CompactDictionaryView, Dictionary, DictionaryView,
    };
    use crate::encoding::config::{FixedThreshold, ThresholdSpec, TrainingConfig};
    use crate::encoding::trainer::train;
    use crate::{DEFAULT_CONFIG, MaxDictBits};

    fn encode_strings<O: Offset>(
        bytes: &[u8],
        offsets: &[O],
        lpm: &LongestPrefixMatcher,
    ) -> (Vec<Token>, Vec<O>) {
        let rows = ArrowRows::new(bytes, offsets);
        let mut codes = Vec::new();
        let mut row_offsets = vec![O::from_usize(0)];
        for i in 0..rows.num_rows() {
            let row = rows.row(i);
            let mut pos = 0;
            while pos < row.len() {
                let (tok, mlen) = lpm.find_longest_match(&row[pos..]);
                codes.push(tok);
                pos += mlen;
            }
            row_offsets.push(O::from_usize(codes.len()));
        }
        (codes, row_offsets)
    }

    use crate::test_corpus::{
        alternating_strings as make_alternating_strings, binary_strings as make_binary_strings,
        homogeneous_strings as make_homogeneous_strings, make_raw,
        mixed_length_strings as make_mixed_length_strings,
        random_ascii_strings as make_random_strings, user_strings as make_user_strings,
    };

    fn make_base_dict() -> CompactDictionary {
        let mut bytes = Vec::new();
        let mut offsets = vec![0u32];
        for i in 0u16..=255 {
            bytes.push(i as u8);
            offsets.push(bytes.len() as u32);
        }
        CompactDictionary::from_raw(bytes, offsets)
    }

    /// Decode the whole flat code stream against `dict`.
    fn decode_all(codes: &[Token], dict: CompactDictionaryView<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        for &c in codes {
            out.extend_from_slice(dict.token(c));
        }
        out
    }

    /// Decode the codes for row `idx` against `dict`.
    fn decode_row(
        codes: &[Token],
        row_offsets: &[u32],
        dict: CompactDictionaryView<'_>,
        idx: usize,
    ) -> Vec<u8> {
        let begin = row_offsets[idx] as usize;
        let end = row_offsets[idx + 1] as usize;
        let mut out = Vec::new();
        for &c in &codes[begin..end] {
            out.extend_from_slice(dict.token(c));
        }
        out
    }

    fn roundtrip_all<S: AsRef<[u8]>>(strings: &[S], max_dict_bits: u8, seed: u64) -> bool {
        if strings.is_empty() {
            return true;
        }
        let raw = make_raw(strings);
        let cfg = TrainingConfig {
            max_dict_bits,
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(seed),
        };
        let TrainResult { dict, lpm } = train(&ArrowRows::new(&raw.data, &raw.offsets), &cfg);
        let (codes, _) = encode_strings(&raw.data, &raw.offsets, &lpm);
        decode_all(&codes, dict.as_view()) == raw.data
    }

    const WIDTHS: &[u8] = &[9, 10, 11, 12, 13, 14, 15, 16];

    #[test]
    fn zero_strings_produces_no_codes() {
        let lpm = LongestPrefixMatcher::new();
        let (codes, row_offsets) = encode_strings::<u32>(&[], &[0], &lpm);
        assert!(codes.is_empty());
        assert_eq!(row_offsets, vec![0u32]);
    }

    #[test]
    fn single_empty_string_produces_no_codes() {
        let lpm = LongestPrefixMatcher::new();
        let (codes, row_offsets) = encode_strings::<u32>(&[], &[0, 0], &lpm);
        assert!(codes.is_empty());
        assert_eq!(row_offsets, vec![0u32, 0]);
    }

    #[test]
    fn row_offsets_delimit_each_row() {
        let lpm = LongestPrefixMatcher::new();
        let d = make_base_dict();
        let strings: &[&[u8]] = &[b"alpha", b"", b"beta beta", b"gamma"];
        let raw = make_raw(strings);
        let (codes, row_offsets) = encode_strings(&raw.data, &raw.offsets, &lpm);

        assert_eq!(row_offsets.len(), strings.len() + 1);
        assert_eq!(row_offsets[0], 0);
        assert_eq!(*row_offsets.last().unwrap() as usize, codes.len());
        for w in row_offsets.windows(2) {
            assert!(w[1] >= w[0], "row_offsets must be monotonic");
        }
        for (i, s) in strings.iter().enumerate() {
            assert_eq!(decode_row(&codes, &row_offsets, d.as_view(), i), *s);
        }
    }

    #[test]
    fn base_tokens_single_known_string() {
        let lpm = LongestPrefixMatcher::new();
        let d = make_base_dict();
        let raw = make_raw(&["Hello, World!"]);
        let (codes, _) = encode_strings(&raw.data, &raw.offsets, &lpm);
        assert_eq!(decode_all(&codes, d.as_view()), b"Hello, World!");
    }

    #[test]
    fn trained_lpm_produces_multi_byte_tokens() {
        let raw = make_raw(&make_homogeneous_strings(50, 40, b'a'));
        let cfg = TrainingConfig {
            max_dict_bits: 16,
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
        };
        let TrainResult { dict: _, lpm } = train(&ArrowRows::new(&raw.data, &raw.offsets), &cfg);
        let (codes, _) = encode_strings(&raw.data, &raw.offsets, &lpm);
        assert!(
            codes.len() < raw.data.len(),
            "parser did not use multi-byte tokens"
        );
    }

    #[test]
    fn rows_and_contiguous_input_agree() {
        let corpus: Vec<Vec<u8>> = make_user_strings(200)
            .into_iter()
            .map(String::into_bytes)
            .collect();
        let raw = make_raw(&corpus);
        let rows: Vec<&[u8]> = corpus.iter().map(Vec::as_slice).collect();

        for bits in WIDTHS {
            let cfg = Config {
                max_dict_bits: MaxDictBits::new(*bits).unwrap(),
                ..DEFAULT_CONFIG
            };
            let flat = Parser::train(&raw.data, &raw.offsets, cfg).unwrap();
            let by_rows = Parser::train_rows(rows.as_slice(), cfg);
            assert_eq!(flat.dict.bytes(), by_rows.dict.bytes(), "bits={bits}");

            let a: Column<u32> = flat.parse(&raw.data, &raw.offsets).unwrap();
            let b: Column<u32> = by_rows.parse_rows(rows.as_slice());
            assert_eq!(a.codes, b.codes, "bits={bits}");
            assert_eq!(a.row_offsets, b.row_offsets, "bits={bits}");
        }
    }

    #[test]
    fn validate_offsets_rejects_empty_and_overflow() {
        assert_eq!(validate_offsets::<u32>(b"abc", &[]), Err(Error::InvalidArg));
        assert_eq!(validate_offsets(b"abc", &[0u32, 4]), Err(Error::InvalidArg));
        assert_eq!(validate_offsets(b"abc", &[0u32, 3]), Ok(()));
    }

    #[test]
    fn nonzero_start_offset_roundtrips() {
        let bytes = b"skipalphabetaunused";
        let offsets = [4u32, 9, 13];
        let column = crate::compress(bytes, &offsets, DEFAULT_CONFIG).unwrap();
        let dict = column.dict.as_view();

        assert_eq!(
            decode_row(&column.codes, &column.row_offsets, dict, 0),
            b"alpha"
        );
        assert_eq!(
            decode_row(&column.codes, &column.row_offsets, dict, 1),
            b"beta"
        );
    }

    #[test]
    fn roundtrip_user_strings() {
        for &bits in WIDTHS {
            assert!(roundtrip_all(&make_user_strings(50), bits, 42));
        }
    }

    #[test]
    fn roundtrip_random_ascii_strings() {
        for &bits in WIDTHS {
            assert!(roundtrip_all(&make_random_strings(60, 50, 1337), bits, 42));
        }
    }

    #[test]
    fn roundtrip_binary_strings_with_nul_bytes() {
        for &bits in WIDTHS {
            assert!(roundtrip_all(&make_binary_strings(40, 30, 777), bits, 42));
        }
    }

    #[test]
    fn roundtrip_homogeneous_strings() {
        for &bits in WIDTHS {
            assert!(roundtrip_all(
                &make_homogeneous_strings(30, 40, b'a'),
                bits,
                42
            ));
        }
    }

    #[test]
    fn roundtrip_alternating_strings() {
        for &bits in WIDTHS {
            assert!(roundtrip_all(&make_alternating_strings(30, 40), bits, 42));
        }
    }

    #[test]
    fn roundtrip_mixed_length_strings() {
        for &bits in WIDTHS {
            assert!(roundtrip_all(
                &make_mixed_length_strings(80, 100, 31415),
                bits,
                42
            ));
        }
    }
}
