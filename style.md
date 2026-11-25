# ReifyDB style guide

Every rule applies to all Rust code in `crates`, `bin` and `pkg`: production, tests and
benches alike. A rule that differs for tests says so itself.

`make tidy` lists every violation. `make tidy-changed` checks only what you touched.

## F1 - a function steers or works, never both

**Rule.** A steering function calls helpers. A working function loops, branches and computes.
No function does both.

A steering function may hold flow that only picks which helper runs:
- a guard with an early return
- a `match` that dispatches to helpers
- a loop whose body is one call

**Why.** A steering function reads as a list of steps. A reader learns what happens
without reading how.

**Bad.**
```rust
fn apply(&mut self, change: Change) -> Result<()> {
	let rows = apply_decode(&change)?;
	for row in &rows {
		if row.due < self.now {
			self.index.remove(&row.key);
		}
	}
	self.store.write(rows)
}
```

**Good.**
```rust
fn apply(&mut self, change: Change) -> Result<()> {
	let rows = apply_decode(&change)?;
	self.apply_drop_expired(&rows);
	self.store.write(rows)
}
```

**Enforced by.** Review. F2, F3, F4, F5 and F7 are the machine alarms for it.

## F2 - a function is at most 60 lines

**Rule.** A function body is at most 60 lines.

**Why.** A function that fits one screen is read without scrolling. A longer one is
almost always breaking F1.

**Fix.** Split by F1: move each block of work into a named helper.

**Enforced by.** `tidy`.

## F3 - a function nests at most 3 levels deep

**Rule.** Blocks inside a function body nest at most 3 levels deep.

**Why.** Each level is one more condition the reader must hold in their head. Deep
nesting is a working function doing several jobs.

**Bad.**
```rust
fn expire(&mut self, rows: &[Row]) {
	for row in rows {
		if row.due < self.now {
			match row.kind {
				Kind::Timer => {
					if self.armed {
						self.index.remove(&row.key);
					}
				}
				Kind::Plain => {}
			}
		}
	}
}
```

**Good.**
```rust
fn expire(&mut self, rows: &[Row]) {
	for row in rows {
		if row.due >= self.now {
			continue;
		}
		self.expire_row(row);
	}
}
```

**Fix.** Turn conditions into guards with an early return or `continue`. If it is still
too deep, move the inner block into a helper.

**Enforced by.** `tidy`.

## F4 - a function takes at most 4 arguments

**Rule.** A function takes at most 4 arguments, `self` included.

**Why.** A crowded signature means the function does several jobs, or values that
travel together are missing a name.

**Bad.**
```rust
fn scan(&self, shape: &RowShape, start: Key, end: Key, limit: usize, now: DateTime) { .. }
```

**Good.**
```rust
fn scan(&self, shape: &RowShape, request: ScanRequest) { .. }
```

**Fix.** Group values that travel together into a named struct, or split the function by F1.

**Exception.** `extern "C"` functions are exempt.

**Enforced by.** `tidy`.

## F5 - a function takes no bool arguments

**Rule.** A function takes no `bool` argument.

**Why.** A bool argument is two functions in one body, which breaks F1. At the call
site `scan(range, true)` says nothing about what `true` means.

**Bad.**
```rust
fn scan(&self, range: KeyRange, reverse: bool) { .. }
```

**Good.**
```rust
fn scan(&self, range: KeyRange, order: Order) { .. }
```

**Fix.** Use an enum with two named variants, or split into two functions.

**Enforced by.** `tidy`.

## F6 - guards come first and the happy path is not indented

**Rule.** A condition that ends the work comes first and leaves with `return`, `continue`
or `break`. No `else` follows a branch that leaves. The work the function exists for
stays at the left edge of its body.

An `else` is for two real alternatives, not for the case after a guard.

**Why.** A reader meets the reasons to stop one at a time and then reads the work
without holding any of them. Nested, every condition stays open until the last brace.

**Bad.**
```rust
fn expire_row(&mut self, row: &Row) {
	if let Some(timer) = self.timers.get(&row.key) {
		if timer.due >= self.now {
			return;
		} else {
			self.index.remove(&row.key);
		}
	}
}
```

**Good.**
```rust
fn expire_row(&mut self, row: &Row) {
	let Some(timer) = self.timers.get(&row.key) else {
		return;
	};
	if timer.due >= self.now {
		return;
	}
	self.index.remove(&row.key);
}
```

**Enforced by.** `tidy` for an `else` after a branch that leaves, and for a condition
that wraps the whole rest of a body. Review for the rest.

## F7 - a function has at most 10 local variables

**Rule.** A function body binds at most 10 names. Every name bound by `let`, `if let` or
`while let` counts, so `let (start, end)` counts two; `_` does not. Arguments, loop
variables and names bound in a `match` arm do not count. A closure's names count toward
the function that holds it.

**Why.** Every local is one more thing the reader tracks. More than 10 means the
function is doing several jobs.

**Fix.** Split by F1: move a group of related locals and the work on them into a helper.

**Enforced by.** `tidy`.

## F8 - callers come before the helpers they call

**Rule.** Within one `impl` block or one module, a function sits above the helpers it
calls, and its helpers follow in the order they are first called.

A helper with several callers sits below the first of them.

**Why.** The steering function is the summary. A file in call order reads top to bottom
the same way the steering function does.

**Bad.**
```rust
fn apply_decode(change: &Change) -> Result<Vec<Row>> { .. }

fn apply(&mut self, change: Change) -> Result<()> {
	let rows = apply_decode(&change)?;
	self.store.write(rows)
}
```

**Good.**
```rust
fn apply(&mut self, change: Change) -> Result<()> {
	let rows = apply_decode(&change)?;
	self.store.write(rows)
}

fn apply_decode(change: &Change) -> Result<Vec<Row>> { .. }
```

**Enforced by.** `tidy`.

## N1 - names are not abbreviated

**Rule.** A name is spelled out in full. This holds for variables, arguments, fields,
functions, types and modules, public or private.

An abbreviation is any word on the banned list below. The list grows when a new one is found.

| banned | write instead |
|---|---|
| txn | transaction |
| ctx | context |
| col | column |
| cols | columns |
| buf | buffer |
| src | source |
| dst | destination |
| val | value |
| vals | values |
| msg | message |
| idx | index |
| pos | position |
| ptr | pointer |
| req | request |
| resp | response |
| num | number |
| cnt | count |
| acc | accumulator, or what it accumulates: total, sum |
| res | result |
| tmp | what the value is: previous, swapped |
| cfg | config |

`len` is not on the list. It is the standard library's own word, and our types offer
`len()` the same way.

**Why.** A reader should not have to learn a private shorthand. A full word is read
once; a short form is decoded every time.

**Bad.**
```rust
fn scan(&self, txn: &Txn, ctx: &Ctx, col: &Col) { .. }
```

**Good.**
```rust
fn scan(&self, transaction: &Transaction, context: &Context, column: &Column) { .. }
```

**Enforced by.** `tidy`.

## N2 - single-letter names are for a loop index and a generic parameter only

**Rule.** A name has more than one letter. Two exceptions:
- the index of a `for` loop over a range, and the index half of `.enumerate()`
- a generic type parameter

**Why.** A single letter says nothing about what the value is. An index and a generic
parameter are the two places where the letter is the convention and the meaning is
given by the position.

**Bad.**
```rust
fn scan(&self, rows: &[Row], entries: &[Entry]) {
	let r = self.read();
	rows.iter().map(|e| e.id);
	for e in entries { .. }
}

fn key(k: Key) { .. }
```

**Good.**
```rust
fn scan(&self, rows: &[Row], entries: &[Entry]) {
	let page = self.read();
	rows.iter().map(|entry| entry.id);
	for entry in entries { .. }
	for i in 0..rows.len() { .. }
	for (i, row) in rows.iter().enumerate() { .. }
}

fn key(key: Key) { .. }

fn run<T: Row>(row: T) { .. }
```

**Enforced by.** `tidy`.

## N3 - a helper is named after its caller

**Rule.** A private function called by exactly one function in the same `impl` block or
module starts with that caller's name.

A private function with several callers carries no caller prefix.

A helper chain deeper than two levels moves into its own module.

**Why.** The name shows where a helper belongs. With F8, a file reads as groups: a
function, then the helpers that carry its name.

**Bad.**
```rust
fn apply(&mut self, change: Change) -> Result<()> {
	let rows = decode(&change)?;
	self.drop_expired(&rows);
	self.store.write(rows)
}

fn decode(change: &Change) -> Result<Vec<Row>> { .. }

fn drop_expired(&mut self, rows: &[Row]) { .. }
```

**Good.**
```rust
fn apply(&mut self, change: Change) -> Result<()> {
	let rows = apply_decode(&change)?;
	self.apply_drop_expired(&rows);
	self.store.write(rows)
}

fn apply_decode(change: &Change) -> Result<Vec<Row>> { .. }

fn apply_drop_expired(&mut self, rows: &[Row]) { .. }
```

**Enforced by.** `tidy`.

## N4 - a lookup verb says whether the value can be absent and where it comes from

**Rule.** A function that hands back a value uses one of four verbs:

| verb | meaning | returns |
|---|---|---|
| `get_` | in memory, always there | the value |
| `find_` | in memory, may be absent | `Option` |
| `read_` | goes to storage | `Result` |
| `load_` | reads from storage and builds a whole thing | `Result` |

`lookup_` and `fetch_` are not used. `resolve_` (a name to its object) and `query_` (runs
a query) keep their own meaning.

**Why.** A caller always needs to know two things: can this come back empty, and does it
touch storage. The verb answers both without opening the function.

**Bad.**
```rust
fn get_table(&self, name: &str) -> Option<&Table> { .. }

fn lookup_table(&self, id: TableId) -> &Table { .. }

fn fetch_row(&self, key: &Key) -> Result<Option<Row>, StoreError> { .. }
```

**Good.**
```rust
fn find_table(&self, name: &str) -> Option<&Table> { .. }

fn get_table(&self, id: TableId) -> &Table { .. }

fn read_row(&self, key: &Key) -> Result<Option<Row>, StoreError> { .. }

fn load_catalog(&self) -> Result<Catalog, StoreError> { .. }
```

**Enforced by.** `tidy` for the two unused verbs and for `find_` returning `Option` and
`get_` not returning it. Review for memory against storage.

## N5 - a making verb says what kind of making it is

**Rule.** A function that makes something uses one of five verbs:

| verb | meaning |
|---|---|
| `new` | the plain constructor |
| `with_` | a constructor variant |
| `from_` | made from another representation |
| `build_` | assembles a value and changes nothing outside the function |
| `create_` | the thing now exists and is stored |

`make_` and `new_` followed by a word are not used. `open` (attach to something that
exists) and `init` (set up state) keep their own meaning.

**Why.** `create` is a word the user writes in RQL. In the code it means the same thing:
something now exists and is stored. A caller must be able to tell that from a value that
is only assembled in memory.

**Bad.**
```rust
fn create_plan(&self, query: &Query) -> Plan { .. }

fn make_buffer(capacity: usize) -> Buffer { .. }

fn new_empty() -> Buffer { .. }
```

**Good.**
```rust
fn build_plan(&self, query: &Query) -> Plan { .. }

fn with_capacity(capacity: usize) -> Buffer { .. }

fn new() -> Buffer { .. }

fn create_table(&mut self, table: TableDef) -> Result<TableId, CatalogError> { .. }
```

**Enforced by.** `tidy` for the two unused verbs. Review for stored against assembled.

## N6 - a boolean reads as a statement and names the positive case

**Rule.** A boolean name reads as something that is true or false at the point of use.
- A function that returns `bool` asks a question: `is_`, `has_`, `can_`, or a verb that
  already is one, like `contains` or `matches`.
- A field or a local is an adjective or a past participle: `nullable`, `exhausted`,
  `enabled`. A noun, a verb or an adverb is not a boolean name.

A boolean always names the positive case. It is never negated: no `not_`, `no_`, `non_`
or `is_not_`, and no word that means the absence of the good case when its opposite
exists (`valid`, not `invalid`; `enabled`, not `disabled`).

**Why.** `if page.more` needs a second look; `if page.has_more` does not. A negated name
makes every use a double negative: `if !not_found` has to be worked out, `if found` is
read.

**Bad.**
```rust
struct Page {
	more: bool,
	not_found: bool,
	disabled: bool,
}

fn empty(&self) -> bool { .. }

fn is_not_ready(&self) -> bool { .. }
```

**Good.**
```rust
struct Page {
	has_more: bool,
	found: bool,
	enabled: bool,
}

fn is_empty(&self) -> bool { .. }

fn is_ready(&self) -> bool { .. }
```

**Enforced by.** `tidy` for the negating prefixes. Review for the rest.

## N7 - the type carries the unit

**Rule.** A value with a unit is a typed value, and its name has no unit suffix. Time is
always typed: `Duration` for elapsed time, `DateTime` for an instant, `Date` and `Time`
for their halves.

Where no typed value exists (bytes, rows), the unit is a whole word at the end of the
name. It is never abbreviated.

**Why.** A raw number with the unit in its name relies on every caller reading the
suffix. `timeout_ms` passed where `timeout_us` is expected compiles and is wrong by a
factor of a thousand.

**Bad.**
```rust
struct Limits {
	timeout_ms: u64,
	ttl_us: i64,
	size_kb: usize,
}
```

**Good.**
```rust
struct Limits {
	timeout: Duration,
	ttl: Duration,
	size_bytes: usize,
}
```

**Enforced by.** `tidy` for abbreviated unit suffixes. Review for the rest.

## M1 - a file is at most 500 lines

**Rule.** A source file is at most 500 lines. A test file has no limit.

A test file is a `tests.rs`, or any file under a `tests` or `benches` directory. Every
other file is a source file, whatever its name.

**Why.** F2 keeps a function to one screen; nothing else keeps a file to one subject. A
file that outgrows the limit holds more than one thing and is split along that line.

**Fix.** Turn the file into a directory module and give each subject its own file.

**Enforced by.** `tidy`.

## M2 - unit tests live in their own file

**Rule.** A source file holds no test code. Its unit tests sit in a `tests.rs` file
beside it, declared with `#[cfg(test)] mod tests;` as the last line of the module.

**Why.** A code file then holds only code, and its length is the length of the code. The
tests of a module are found in one known place.

**Bad.**
```rust
fn scan(&self) { .. }

#[cfg(test)]
mod tests {
	#[test]
	fn scan_returns_rows_in_key_order() { .. }
}
```

**Good.**
```rust
fn scan(&self) { .. }

#[cfg(test)]
mod tests;
```

**Fix.** `scan.rs` becomes `scan/mod.rs` and `scan/tests.rs`.

**Enforced by.** `tidy`.

## M3 - a directory module is a `mod.rs` that holds the module's main item

**Rule.** A module with child files is a directory with a `mod.rs`. The `mod.rs` holds
the `mod` lines and the main type or entry function of the module. No `scan.rs` sits
beside a `scan` directory.

**Why.** One layout means a module is always found the same way. With the main item in
`mod.rs` the path stays `scan::Scan`, not `scan::scan::Scan`.

**Good.**
```text
scan/
    mod.rs       mod cursor; mod merge; struct Scan and its impl
    cursor.rs
    merge.rs
    tests.rs
```

**Enforced by.** `tidy` for a file beside a directory of the same name. Review for the
rest.

## M4 - an item is as visible as it must be, in three levels

**Rule.** An item carries the narrowest marker that compiles:

| marker | used by |
|---|---|
| none | this module |
| `pub(crate)` | elsewhere in this crate |
| `pub` | another crate |

`pub(super)` and `pub(in ..)` are not used.

**Why.** `pub` then means the API of the crate and nothing else. The compiler warns about
an unused `pub(crate)` item and never about an unused `pub` one, so a narrow marker is
what makes dead code show up.

**Bad.**
```rust
pub fn merge_runs(runs: &[Run]) -> Run { .. }

pub(super) fn open_tier(path: &Path) -> Result<Tier, StoreError> { .. }
```

**Good.**
```rust
fn merge_runs(runs: &[Run]) -> Run { .. }

pub(crate) fn open_tier(path: &Path) -> Result<Tier, StoreError> { .. }
```

**Enforced by.** `tidy` for the two unused markers. The `unreachable_pub` lint and review
for the rest.

## D1 - a bool field is a state, a choice is an enum

**Rule.** A field is a `bool` only when it holds a state that is true or false:
`exhausted`, `nullable`, `enabled`. A choice between two ways of doing something is an
enum that names both ways.

**Why.** `cascade: false` does not say what happens instead. An enum names the other way,
takes a third one without touching every user, and passes F5 when the value becomes an
argument.

**Bad.**
```rust
struct Sort {
	descending: bool,
}

struct Drop {
	cascade: bool,
	if_exists: bool,
}
```

**Good.**
```rust
struct Sort {
	direction: Direction,
}

struct Drop {
	dependents: Dependents,
	missing: Missing,
}

enum Direction {
	Ascending,
	Descending,
}

enum Dependents {
	Cascade,
	Restrict,
}

enum Missing {
	Fail,
	Skip,
}

struct Page {
	exhausted: bool,
}
```

**Enforced by.** Review.

## D2 - a struct is plain data or it guards a rule, never half

**Rule.** A struct is one of two kinds:
- Plain data: any combination of field values is valid. Its fields are open, at the
  level M4 gives them.
- Guarded: a rule holds between its fields. All fields are private, it is built by a
  constructor and read through methods.

A struct does not mix open and private fields.

**Why.** An open field can be set to anything from anywhere. If a rule holds between
fields, one place has to enforce it, and that only works when no field is open.

**Bad.**
```rust
pub struct Page {
	pub rows: Vec<Row>,
	count: usize,
}
```

**Good.**
```rust
pub struct TableDef {
	pub name: String,
	pub columns: Vec<ColumnDef>,
}

pub struct Page {
	rows: Vec<Row>,
	count: usize,
}

impl Page {
	pub fn new(rows: Vec<Row>) -> Page { .. }

	pub fn rows(&self) -> &[Row] { .. }
}
```

**Enforced by.** `tidy` for a struct that mixes open and private fields. Review for the
rest.

## D3 - values that could be swapped have their own types

**Rule.** An identifier is never a raw integer. Each kind has its own type, and the name
drops the `_id` suffix: `table: TableId`.

The same holds for any two arguments of one function that share a type and could be
passed in the wrong order: each gets a type that names what it is.

**Why.** Two raw values of the same type can be swapped and the code still compiles. In a
database that reads or writes the wrong object.

**Bad.**
```rust
fn find_column(&self, table_id: u64, column_id: u64) -> Option<&Column> { .. }

fn slice(&self, offset: usize, limit: usize) -> &[Row] { .. }
```

**Good.**
```rust
struct TableId(u64);

struct ColumnId(u64);

struct Offset(usize);

struct Limit(usize);

fn find_column(&self, table: TableId, column: ColumnId) -> Option<&Column> { .. }

fn slice(&self, offset: Offset, limit: Limit) -> &[Row] { .. }
```

**Exception.** `extern "C"` functions take the raw values their ABI demands.

**Enforced by.** `tidy` for a name ending in `id` with a raw integer type. Review for the
rest.

## D4 - a number is converted with `from` or `try_from`, never with `as`

**Rule.** A conversion between number types that cannot fail uses `from`. One that can
fail uses `try_from`, and the error propagates under E1. `as` is not used to convert to
an integer type.

**Why.** `as` never fails. A value that does not fit is cut off or wrapped and the code
carries on with it. A cut-off offset or length points at the wrong bytes.

**Bad.**
```rust
fn slot(&self, position: u64) -> u32 {
	position as u32
}
```

**Good.**
```rust
fn slot(&self, position: u64) -> Result<u32, TryFromIntError> {
	u32::try_from(position)
}

fn widen(&self, slot: u32) -> u64 {
	u64::from(slot)
}
```

**Exception.** An integer converted to a float: `hits as f64`.

**Enforced by.** `tidy`.

## E1 - an unexpected error propagates and stops the application

**Rule.** An unexpected error travels up with `?` through every layer. At the top of the
call chain (an actor tick, a subsystem entry point, a worker loop) it becomes a panic whose
message names what failed. It is never turned into a default, a fallback or a log line.

**Why.** A swallowed error keeps the system running on wrong data. What does not stop
does not get fixed.

**Bad.**
```rust
fn tick(&mut self) {
	let objects = self.scan_tier().unwrap_or_default();
	self.metrics.replace(objects);
}
```

**Good.**
```rust
fn tick(&mut self) {
	if let Err(error) = self.tick_refresh() {
		panic!("metrics refresh failed: {error}");
	}
}

fn tick_refresh(&mut self) -> Result<(), StoreError> {
	let objects = self.scan_tier()?;
	self.metrics.replace(objects);
	Ok(())
}
```

**Enforced by.** Review.

## E2 - only an expected error is handled

**Rule.** An error is handled without stopping only when it is part of the contract: a
lookup that finds nothing, a missing optional row, a closed channel during shutdown.
Everything else is unexpected and falls under E1.

**Why.** Handling is a statement that the case is normal. Making it for a case that is
not normal hides a defect.

**Bad.**
```rust
fn load(&self, key: &Key) -> Option<Row> {
	self.store.read(key).ok()
}
```

**Good.**
```rust
use crate::error::StoreError::NotFound;

fn load(&self, key: &Key) -> Result<Option<Row>, StoreError> {
	match self.store.read(key) {
		Err(NotFound) => Ok(None),
		other => other.map(Some),
	}
}
```

**Enforced by.** Review.

## E3 - a result names its error type

**Rule.** A function returns `Result<T, E>` with the concrete error type written out.
There is no `Result` type alias.

**Why.** An alias hides which errors a function can return. The reader has to look it up
in every module.

**Bad.**
```rust
type Result<T> = std::result::Result<T, StoreError>;

fn load(&self, key: &Key) -> Result<Row> { .. }
```

**Good.**
```rust
fn load(&self, key: &Key) -> Result<Row, StoreError> { .. }
```

**Enforced by.** `scripts/check-result-aliases.sh`.

## E4 - source never unwraps, and expects only what cannot happen

**Rule.** A source file has no `unwrap()`. `expect` is used only for a case that cannot
happen, and its message states why. An error that can happen propagates with `?` under
E1; it is never an `expect`.

In a test file both are free.

**Why.** A bare `unwrap` that fires tells a line number and nothing else, and hides
whether the author meant "this cannot happen" or did not think about it. An `expect`
with its reason records what the author knows to be true.

**Bad.**
```rust
fn last_page(&self) -> &Page {
	self.pages.last().unwrap()
}

fn load(&self, key: &Key) -> Row {
	self.store.read(key).expect("read failed")
}
```

**Good.**
```rust
fn last_page(&self) -> &Page {
	self.pages.last().expect("a store always has one page")
}

fn load(&self, key: &Key) -> Result<Row, StoreError> {
	self.store.read(key)
}
```

**Enforced by.** `tidy` for `unwrap()` in a source file. Review for the rest.

## T1 - a test fails when the behaviour it protects breaks

**Rule.** A test asserts the outcome that matters, strictly. A test that still passes
after the logic it covers is changed is wrong. Failure paths and contract edges are
tested alongside the happy path.

**Why.** A test that cannot fail protects nothing and reads as protection.

**Bad.**
```rust
#[test]
fn scan_runs() {
	let rows = scan(&store()).unwrap();
	assert!(!rows.is_empty());
}
```

**Good.**
```rust
#[test]
fn scan_returns_rows_in_key_order() {
	// A reader merges on key order, so an unordered scan must fail here.
	let rows = scan(&store()).unwrap();
	assert_eq!(keys(&rows), [1, 2, 3]);
}
```

**Enforced by.** Review.

## T2 - a failing test is answered in the implementation

**Rule.** A test is never weakened, ignored, deleted or rewritten to make it pass. The
first question for a red test is what is wrong with the implementation.

**Why.** A relaxed assertion turns a found defect into a hidden one.

**Bad.**
```rust
#[test]
#[ignore]
fn scan_returns_rows_in_key_order() { .. }
```

**Good.**
```rust
#[test]
fn scan_returns_rows_in_key_order() { .. }
```

**Enforced by.** Review.

## T3 - a test explains itself from the inside

**Rule.** The comment that explains a test sits on the first lines inside the test body,
below the signature. Nothing is written above `#[test]`. The same holds for test helpers
and fixtures.

**Why.** A comment above the attribute drifts away from the code when attributes are
added, and reads as file-level narration in a diff.

**Bad.**
```rust
// An unordered scan must fail here.
#[test]
fn scan_returns_rows_in_key_order() { .. }
```

**Good.**
```rust
#[test]
fn scan_returns_rows_in_key_order() {
	// A reader merges on key order, so an unordered scan must fail here.
	..
}
```

**Enforced by.** Review.

## T4 - a regression test is a snapshot file

**Rule.** A regression test is a testscript snapshot file under a `regression/`
directory, not an inline Rust test.

**Why.** The script shows the exact input that broke and the exact output expected, in
the language the user wrote it in.

**Enforced by.** Review.

## C1 - non-test source carries no comments

**Rule.** A source file outside tests has no line, block or doc comment. Names and
structure carry the meaning.

**Why.** A comment is a second copy of the code that nothing keeps true. If code needs
explaining, the name or the shape is wrong.

**Bad.**
```rust
// Drop rows whose timer has run out.
fn sweep(&mut self) { .. }
```

**Good.**
```rust
fn drop_expired_rows(&mut self) { .. }
```

**Exception.** `// SAFETY:` on `unsafe`, see C2. License headers and tool directives.

**Enforced by.** Review.

## C2 - every unsafe states its safety contract

**Rule.** Every `unsafe` block carries a `// SAFETY:` comment, and every `unsafe fn` a
`# Safety` doc section. It states the invariants the code relies on: alignment, lifetime
and ownership, bounds, validity. Nothing else.

**Why.** The compiler stops checking inside `unsafe`. The comment is the only record of
what the author checked instead.

**Bad.**
```rust
fn first(bytes: &[u8]) -> u8 {
	unsafe { *bytes.get_unchecked(0) }
}
```

**Good.**
```rust
fn first(bytes: &[u8]) -> u8 {
	assert!(!bytes.is_empty());
	// SAFETY: the assert above guarantees index 0 is in bounds.
	unsafe { *bytes.get_unchecked(0) }
}
```

**Enforced by.** Review.

## C3 - a test comment is one line and names a failure or an invariant

**Rule.** A comment in a test is one line. It names a failure mode or an invariant, with
words like "without", "otherwise", "must", "never", "exactly". It holds no file name, no
test name, no history, and does not restate the code below it.

**Why.** The why of a test is what a reader cannot get from the code. Everything else in
a comment is narrative that goes stale.

**Bad.**
```rust
#[test]
fn scan_returns_rows_in_key_order() {
	// This test was added after the bug in scan.rs.
	// It calls scan and then checks the keys.
	..
}
```

**Good.**
```rust
#[test]
fn scan_returns_rows_in_key_order() {
	// A reader merges on key order, so an unordered scan must fail here.
	..
}
```

**Enforced by.** Review.

## C4 - no section divider comments

**Rule.** No banner comment groups code into sections.

**Why.** If a file needs headings, it holds more than one thing. The items show the
structure.

**Bad.**
```rust
// ---------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------
fn store() -> Store { .. }
```

**Good.**
```rust
fn store() -> Store { .. }
```

**Enforced by.** `scripts/check-section-comments.sh`.

## C5 - source is plain ASCII punctuation, no em-dashes

**Rule.** No em-dash appears in a source file: not in code, comments, messages or
Markdown inside the repository. Use a hyphen, parentheses, a semicolon, or rephrase.

**Why.** It cannot be typed on a keyboard and does not show up in a search for `-`.

**Enforced by.** `scripts/check-em-dashes.sh`.

## C6 - a comment holds no plan or decision id

**Rule.** No comment refers to a plan, a phase, a step or a decision id.

**Why.** The plan is gone long before the code is. The id then points at nothing.

**Enforced by.** `scripts/check-plan-refs.sh`.

## I1 - no re-exports

**Rule.** An item is imported from the module that defines it. No `pub use` passes it on
from somewhere else.

**Why.** With one path per item, a search finds every use and a reader always knows
where the definition is.

**Bad.**
```rust
pub use crate::tier::range::KeyRange;
```

**Good.**
```rust
use crate::tier::range::KeyRange;
```

**Enforced by.** `scripts/check-internal-reexports.sh`.

## I2 - imports sit at the top of the module

**Rule.** Every `use` is at module level. None is inside a function or a block.

**Why.** One place to read tells what a file depends on.

**Bad.**
```rust
fn scan(&self) {
	use crate::tier::range::KeyRange;
	self.read(KeyRange::all());
}
```

**Good.**
```rust
use crate::tier::range::KeyRange;

fn scan(&self) {
	self.read(KeyRange::all());
}
```

**Enforced by.** `scripts/check-toplevel-imports.sh`.

## I3 - a path is never written inline

**Rule.** Code uses the short name of an item. The full path appears once, in a `use`
at the top.

**Why.** A long path in the middle of a line hides what the line does.

**Bad.**
```rust
fn index(&self) -> std::collections::HashMap<Key, Row> { .. }
```

**Good.**
```rust
use std::collections::HashMap;

fn index(&self) -> HashMap<Key, Row> { .. }
```

**Enforced by.** `scripts/check-inline-qualifications.sh`.

## I4 - enum variants are imported, not qualified, in a match

**Rule.** A module that matches on an enum imports its variants and uses the bare names
in the arms.

**Why.** The arms then read as the cases themselves, without the type name repeated on
every line.

**Bad.**
```rust
fn expire_row(&mut self, row: &Row) {
	match row.kind {
		Kind::Timer => self.index.remove(&row.key),
		Kind::Plain => {}
	}
}
```

**Good.**
```rust
use crate::row::Kind::{Plain, Timer};

fn expire_row(&mut self, row: &Row) {
	match row.kind {
		Timer => self.index.remove(&row.key),
		Plain => {}
	}
}
```

**Enforced by.** Review.
