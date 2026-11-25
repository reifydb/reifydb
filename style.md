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

**Enforced by.** Review. F2 to F5 are the machine alarms for it.

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

## F6 - arguments that can be swapped get names

**Rule.** Two or more arguments of the same type that a caller could swap by mistake
go into a named struct.

**Why.** The compiler cannot catch `copy(to, from)`. A struct literal names each value
at the call site.

**Bad.**
```rust
fn copy(&self, from: Key, to: Key) { .. }
```

**Good.**
```rust
fn copy(&self, span: CopySpan) { .. }
```

**Enforced by.** Review.

## F7 - a function has at most 10 local variables

**Rule.** A function body holds at most 10 `let` bindings. Arguments and loop variables
do not count. A closure's bindings count toward the function that holds it.

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
