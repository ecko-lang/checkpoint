# checkpoint - Ecko Std Lib Package

Resumable long-running jobs over `std.sql`. A job that dies at record
800,000,000 starts again at 800,000,000 on the next run, and applies nothing
twice.

Two primitives cover the two shapes of long-running work: `each_batch` walks a
source in batches with a committed cursor, and `step` runs a named pipeline
step once and remembers its result.

No capabilities of its own: every function takes a database handle you opened.

## Install

```bash
ecko get github.com/ecko-lang/checkpoint
```

```ecko
import checkpoint
```

## Usage

A backfill: migrate every record in `gen1_transactions` into
`gen2_transactions`, 10,000 at a time. Kill the process at any point and run
the same program again; it carries on after the last batch that committed.

```ecko
import std.sql
import checkpoint

db = sql.open("ledger.db")

source = checkpoint.sql_source(db, "gen1_transactions", "id")

summary = checkpoint.each_batch(db, "gen2-backfill", source, 10000, fn(batch, tx) {
    for row in batch {
        sql.exec(
            tx,
            "insert into gen2_transactions (id, account_ref, amount_cents) values (?, ?, ?)",
            [row.id, upper(row.account), row.cents],
        )
    }
})
# { batches: 131, items: 1308442, done: true, cursor: 1308442 }
```

A pipeline of steps: a step that finished is skipped on the next run, and its
stored result is returned instead.

```ecko
report = checkpoint.step(db, "nightly/extract", fn() extract())
totals = checkpoint.step(db, "nightly/aggregate", fn() aggregate(report))
```

### The guarantee

`each_batch` runs each batch in one `sql.transaction`: it reads the committed
cursor, calls the source, calls your handler, and stores the new cursor. The
handler's writes and the cursor commit together or not at all. A crash at any
point, including mid-batch, loses at most the batch in flight, and that
batch's writes roll back with it. The next run replays that batch from the
same cursor.

- **Writes to the same database, through the `tx` handle, happen exactly
  once.** This is why the checkpoint table lives in the job's own database and
  the API takes one handle for both.
- **Side effects outside that database happen at least once.** An HTTP call, a
  message on a queue or a write to a second database made from the handler is
  repeated if the process dies after it and before the commit. Give each one a
  stable id the receiver can dedupe on, or record it in the same transaction
  with the `outbox` package and deliver it from there.

`step` gives the same guarantee for one unit of work: its function runs inside
a transaction together with the write that records it, so writes it makes
through `db` commit only if the step is recorded.

## API

| function | what it does |
|---|---|
| `each_batch(db, name, source, limit, handler)` | run job `name` to completion from its last committed cursor; returns `{ batches, items, done, cursor }` for this call |
| `step(db, name, f)` | run `f()` once and store its result; later calls return the stored value |
| `status(db, name)` | the stored state of a job or step, or `null` |
| `reset(db, name)` | forget a job or step so it runs again from the start; returns whether it existed |
| `sql_source(db, table, key, where = null, columns = null)` | a keyset-paginated source over a table, for `each_batch` |

### Sources

A source is any function `fn(cursor, limit)` that returns
`{ items: [...], next: cursor }`. The cursor is `null` on the first call and
whatever the previous batch returned as `next` after that. It is stored as
JSON, so it must be an int, string, list or map of those.

| source | cursor |
|---|---|
| a SQL table, via `sql_source` | the last key read |
| a JSONL file | a byte or line offset |
| a paginated HTTP API | the page token |

A run ends when the source returns no items, or returns items with
`next: null` (a last page with no token). The job is then marked done, and
every later `each_batch` call with that name returns immediately without
calling the source. `reset` is the way to run it again.

`each_batch` refuses a source that returns a non-empty batch without moving its
cursor, since that would loop forever.

### `sql_source`

```ecko
source = checkpoint.sql_source(db, "gen1_transactions", "id")
settled = checkpoint.sql_source(
    db,
    "gen1_transactions",
    "id",
    where: sql { status = {"settled"} },
    columns: ["account", "cents"],
)
```

Each batch is `select ... where key > cursor order by key limit n`: keyset
pagination, never `OFFSET`, so batch 100,000 costs what batch 1 does, and gaps
in the key are harmless.

It is built to be hard to misuse:

- **The key must be unique on its own**: the table's single-column primary key,
  or the only column of a non-partial unique index. This is checked when the
  source is built. Keyset over a non-unique key silently skips the rows that
  share a key across a batch boundary.
- `table`, `key` and `columns` must be plain identifiers; they are quoted into
  the query, never spliced from arbitrary text.
- `where` must be a `sql { ... }` block, so its values bind as parameters.
- `columns` always gets the key added if you left it out.
- Rows whose key is `NULL` are never visited.

### `status`

```ecko
checkpoint.status(db, "gen2-backfill")
# { name: "gen2-backfill", kind: "batch", done: false, cursor: 812000000,
#   value: null, batches: 81200, items: 812000000,
#   started_at: 1790170556460, updated_at: 1790199912033, finished_at: null }
```

`batches` and `items` are totals across every run. Times are milliseconds since
the Unix epoch. For a step, `value` holds the stored result.

## Notes

**Write through `tx`, and do not open a transaction inside the handler.**
`tx` is the same connection as `db`, already inside a transaction. SQLite has
no nested transactions, so calling `sql.transaction` on that handle from
inside a handler raises a `sql` error, and so does a `checkpoint.step` or
`each_batch` that has work to do. The same holds inside a step's function. A write made on a different connection is outside
the transaction and gets the at-least-once guarantee, not exactly-once.

**Step results must survive JSON.** A step's result is stored as JSON and must
decode to a value equal to the one returned: null, bools, ints, floats,
strings, and lists and maps of those. A decimal, bytes, a secret or a record is
refused rather than stored as something else; convert it first (for example
`string(amount)`). Both the first run and a resumed run return the decoded
value, so they see the same thing.

**A step that throws is not stored.** Its writes roll back, the error
re-raises, and the next call runs it again.

**Done is sticky.** A finished job stays finished, even if its source table
grows afterwards. To pick up new rows, run a new job name whose `where` block
starts after the old cursor, or `reset` and rerun a job whose writes are
idempotent.

**`reset` does not undo writes.** It forgets the cursor or the stored result.
If rerunning the job would duplicate rows, clear them in the same program first.

**One worker per job.** Each batch updates the cursor only if no other run has
moved it since the batch began; if one has, the batch rolls back and
`each_batch` raises. Two workers on one job name therefore fail safe rather
than double-applying, but they do not share the work.

**State lives in a `checkpoints` table** in your database, created on first
use. Job and step names share it, so a job and a step cannot have the same
name. A name like `"nightly/extract"` keeps a pipeline's steps together.

## Testing

```bash
ecko test
```

Offline and deterministic: every test uses an in-memory SQLite database, apart
from one that closes and reopens a file database in the temp directory to
simulate a restarted process. A crash is simulated by a handler that throws
after writing, which the transaction treats exactly as a process that died
before `COMMIT`.

## License

MIT
