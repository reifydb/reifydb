// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

/// Test helpers for engines.
#[cfg(test)]
pub mod test {
    use storage::{Engine, EngineMut, Key, Value};
    use regex::Regex;
    use std::error::Error as StdError;
    use std::fmt::Write as _;
    use std::ops::{Bound, RangeBounds};
    use std::result::Result as StdResult;
    use base::encoding::format;
    use base::encoding::format::Formatter;
    use testing::testscript;
    use testing::testscript::Command;

    pub struct Runner<E: Engine + EngineMut> {
        pub engine: E,
    }

    impl<E: Engine + EngineMut> Runner<E> {
        pub fn new(engine: E) -> Self {
            Self { engine }
        }
    }

    impl<E: Engine + EngineMut> testscript::Runner for Runner<E> {
        fn run(
            &mut self,
            command: &Command,
        ) -> StdResult<String, Box<dyn StdError>> {
            let mut output = String::new();
            match command.name.as_str() {
                // remove KEY
                "remove" => {
                    let mut args = command.consume_args();
                    let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                    args.reject_rest()?;
                    self.engine.remove(&key)?;
                }

                // get KEY
                "get" => {
                    let mut args = command.consume_args();
                    let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                    args.reject_rest()?;
                    let value = self.engine.get(&key)?;
                    writeln!(output, "{}", format::Raw::key_maybe_value(&key, value.as_deref()))?;
                }

                // scan [reverse=BOOL] RANGE
                "scan" => {
                    let mut args = command.consume_args();
                    let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                    let range =
                        parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
                    args.reject_rest()?;

                    let items: Vec<_> = if reverse {
                        self.engine.scan(range).rev().collect::<Vec<storage::Result<(Key, Value)>>>()
                    } else {
                        self.engine.scan(range).collect::<Vec<storage::Result<(Key, Value)>>>()
                    };

                    for Ok((key, value)) in items {
                        let fmtkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "{fmtkv}")?;
                    }
                }

                // scan_prefix PREFIX
                "scan_prefix" => {
                    let mut args = command.consume_args();
                    let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                    args.reject_rest()?;
                    let mut scan = self.engine.scan_prefix(&prefix);
                    while let Some((key, value)) = scan.next().transpose()? {
                        let fmtkv = format::Raw::key_value(&key, &value);
                        writeln!(output, "{fmtkv}")?;
                    }
                }

                // set KEY=VALUE
                "set" => {
                    let mut args = command.consume_args();
                    let kv = args.next_key().ok_or("key=value not given")?.clone();
                    let key = decode_binary(&kv.key.unwrap());
                    let value = decode_binary(&kv.value);
                    args.reject_rest()?;
                    self.engine.set(&key, value)?;
                }

                // status
                "status" => {
                    // command.consume_args().reject_rest()?;
                    // writeln!(output, "{:#?}", self.engine.status()?)?;
                    unimplemented!()
                }

                name => return Err(format!("invalid command {name}").into()),
            }
            Ok(output)
        }
    }

    /// Decodes a raw byte vector from a Unicode string. Code points in the
    /// range U+0080 to U+00FF are converted back to bytes 0x80 to 0xff.
    /// This allows using e.g. \xff in the input string literal, and getting
    /// back a 0xff byte in the byte vector. Otherwise, char(0xff) yields
    /// the UTF-8 bytes 0xc3bf, which is the U+00FF code point as UTF-8.
    /// These characters are effectively represented as ISO-8859-1 rather
    /// than UTF-8, but it allows precise use of the entire u8 value range.
    pub fn decode_binary(s: &str) -> Vec<u8> {
        let mut buf = [0; 4];
        let mut bytes = Vec::new();
        for c in s.chars() {
            // u32 is the Unicode code point, not the UTF-8 encoding.
            match c as u32 {
                b @ 0x80..=0xff => bytes.push(b as u8),
                _ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
            }
        }
        bytes
    }

    /// Parses an binary key range, using Rust range syntax.
    pub fn parse_key_range(s: &str) -> StdResult<impl RangeBounds<Vec<u8>>, Box<dyn StdError>> {
        let mut bound = (Bound::<Vec<u8>>::Unbounded, Bound::<Vec<u8>>::Unbounded);
        let re = Regex::new(r"^(\S+)?\.\.(=)?(\S+)?").expect("invalid regex");
        let groups = re.captures(s).ok_or_else(|| format!("invalid range {s}"))?;
        if let Some(start) = groups.get(1) {
            bound.0 = Bound::Included(decode_binary(start.as_str()));
        }
        if let Some(end) = groups.get(3) {
            let end = decode_binary(end.as_str());
            if groups.get(2).is_some() {
                bound.1 = Bound::Included(end)
            } else {
                bound.1 = Bound::Excluded(end)
            }
        }
        Ok(bound)
    }

    // /// Wraps another engine and emits write events to the given channel.
    // pub struct Emit<E: Engine> {
    //     /// The wrapped engine.
    //     inner: E,
    //     /// Sends operation events.
    //     tx: Sender<Operation>,
    // }
    //
    // /// An engine operation emitted by the Emit engine.
    // pub enum Operation {
    //     Delete { key: Vec<u8> },
    //     Flush,
    //     Set { key: Vec<u8>, value: Vec<u8> },
    // }
    //
    // impl<E: Engine> Emit<E> {
    //     pub fn new(inner: E, tx: Sender<Operation>) -> Self {
    //         Self { inner, tx }
    //     }
    // }
    //
    // impl<E: Engine> Engine for Emit<E> {
    //     type ScanIterator<'a>
    //         = E::ScanIterator<'a>
    //     where
    //         E: 'a;
    //
    //     fn flush(&mut self) -> Result<()> {
    //         self.inner.flush()?;
    //         self.tx.send(Operation::Flush)?;
    //         Ok(())
    //     }
    //
    //     fn delete(&mut self, key: &[u8]) -> Result<()> {
    //         self.inner.delete(key)?;
    //         self.tx.send(Operation::Delete { key: key.to_vec() })?;
    //         Ok(())
    //     }
    //
    //     fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    //         self.inner.get(key)
    //     }
    //
    //     fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
    //         self.inner.scan(range)
    //     }
    //
    //     fn scan_dyn(
    //         &mut self,
    //         range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    //     ) -> Box<dyn ScanIterator + '_> {
    //         Box::new(self.scan(range))
    //     }
    //
    //     fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
    //         self.inner.set(key, value.clone())?;
    //         self.tx.send(Operation::Set { key: key.to_vec(), value })?;
    //         Ok(())
    //     }
    //
    //     fn status(&mut self) -> Result<Status> {
    //         self.inner.status()
    //     }
    // }
    //
    // /// An engine that wraps two others and mirrors operations across them,
    // /// panicking if they produce different results. Engine implementations
    // /// should not have any observable differences in behavior.
    // pub struct Mirror<A: Engine, B: Engine> {
    //     pub a: A,
    //     pub b: B,
    // }
    //
    // impl<A: Engine, B: Engine> Mirror<A, B> {
    //     pub fn new(a: A, b: B) -> Self {
    //         Self { a, b }
    //     }
    // }
    //
    // impl<A: Engine, B: Engine> Engine for Mirror<A, B> {
    //     type ScanIterator<'a>
    //         = MirrorIterator<'a, A, B>
    //     where
    //         Self: Sized,
    //         A: 'a,
    //         B: 'a;
    //
    //     fn delete(&mut self, key: &[u8]) -> Result<()> {
    //         self.a.delete(key)?;
    //         self.b.delete(key)
    //     }
    //
    //     fn flush(&mut self) -> Result<()> {
    //         self.a.flush()?;
    //         self.b.flush()
    //     }
    //
    //     fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    //         let a = self.a.get(key)?;
    //         let b = self.b.get(key)?;
    //         assert_eq!(a, b);
    //         Ok(a)
    //     }
    //
    //     fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    //     where
    //         Self: Sized,
    //     {
    //         let a = self.a.scan((range.start_bound().cloned(), range.end_bound().cloned()));
    //         let b = self.b.scan(range);
    //         MirrorIterator { a, b }
    //     }
    //
    //     fn scan_dyn(
    //         &mut self,
    //         range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    //     ) -> Box<dyn ScanIterator + '_> {
    //         let a = self.a.scan(range.clone());
    //         let b = self.b.scan(range);
    //         Box::new(MirrorIterator::<A, B> { a, b })
    //     }
    //
    //     fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
    //         self.a.set(key, value.clone())?;
    //         self.b.set(key, value)
    //     }
    //
    //     fn status(&mut self) -> Result<Status> {
    //         let a = self.a.status()?;
    //         let b = self.b.status()?;
    //         // Only some items are comparable.
    //         assert_eq!(a.keys, b.keys);
    //         assert_eq!(a.size, b.size);
    //         Ok(a)
    //     }
    // }
    //
    // pub struct MirrorIterator<'a, A: Engine + 'a, B: Engine + 'a> {
    //     a: A::ScanIterator<'a>,
    //     b: B::ScanIterator<'a>,
    // }
    //
    // impl<A: Engine, B: Engine> Iterator for MirrorIterator<'_, A, B> {
    //     type Item = Result<(Vec<u8>, Vec<u8>)>;
    //
    //     fn next(&mut self) -> Option<Self::Item> {
    //         let a = self.a.next();
    //         let b = self.b.next();
    //         assert_eq!(a, b);
    //         a
    //     }
    // }
    //
    // impl<A: Engine, B: Engine> DoubleEndedIterator for MirrorIterator<'_, A, B> {
    //     fn next_back(&mut self) -> Option<Self::Item> {
    //         let a = self.a.next_back();
    //         let b = self.b.next_back();
    //         assert_eq!(a, b);
    //         a
    //     }
    // }
}
