# A New Backend for Egglog

This crate contains a new backend for egglog. It does not implement egglog, but
it is meant to solve most of the "low-level" problems needed to implement
egglog. It provides APIs for:

1. A flexible notion of table (think
   [BYODS](https://kmicinski.com/assets/byods.pdf) but supporting WCOJs).
1. Queries on databases of these tables, using [Free Join](https://arxiv.org/abs/2301.10841).

It includes variants of two major tables used in egglog, but these
implementations could just as easily be moved out of this crate: the underlying
query implementation assumes nothing about the tables that is not exposed
through an (object-safe) trait.

In early benchmarks, this implementation is substantially faster than egglog's
current implementation. It achieves this better performance while also being
more general; it will hopefully allow us to explore new encodings for egglog
terms into relational algebra (e.g. timestamps are supported, but not baked in),
and will make it easier for us to prototype improvements to the query engine.

## Why a New Backend?

The existing implementation of egglog is tightly coupled to the initial join
algorithm and feature set. Aside from most developers of this code thinking that
it needs a rewrite, many structural aspects of the code's organization make it
hard to extend:

### Union-finds Are Too Special
egglog is written under the pervasive assumption that a subset of terms are
identifiers within a union-find. A large portion of the codebase is devoted to a
custom _rebuilding_ operation to recanonicalize the database with its own
incremental processing implementation that is _separate_ from seminaive
evaluation used elsewhere. Other portions of the codebase have different `find`
operations sprinkled in in a way that can make it hard to tell which codepaths
rely on canonical identifiers and which do not.  This raises two problems:

1. "Backend" optimizations rarely need to touch the union-find, but because of
the complexity of these invariants it can be hard to validate that a given
change or optimization is actually safe without some modifications to the
union-find.

1. New features like proofs need to work around the fact that there is a whole
special table that we cannot "program." Instead, these approaches reimplement
the union-find _in egglog_, which can have serious performance problems
(particularly when it comes to rebuilding).

A new backend allows us to separate union-finds from the core implementation,
which in turn makes the implementation easier to modularize and maintain. I do
not fully understand the current plans for proofs, but I believe that a flexible
notion of table now makes proofs a more straightforward project to implement
(albeit, partially _in Rust_ rather than just in egglog).

### Closer to Datalog
The proofs mentioned above are just one example of a number of areas where we
could take egglog that take it beyond the model from the initial implementation.
Egglog is _almost_ Datalog, which is its own rich area where we could have a lot
more dialog and overlap. A separate backend lets us carve out "the version of
Datalog that we need for egglog" and then potentially more directly borrow /
extend techniques from the Datalog literature.

### Separation of Concerns
Egglog is growing in complexity, both as an implementation and as an ecosystem.
There are a large number of ideas for how to extend the language, and how to
expose that language to users. To name a few:

1. Expose a version of egglog as a `#lang` in Racket.
1. Use egglog via python bindings.
1. Use egglog as a Rust library.
1. Add generics and higher-order functions to egglog.

Many of these could be implemented on top of the current _frontend_, but it's
not clear that all of them can be. Separating a backend allows new "frontends"
and semantics for egglog to share a common implementation of joins, indexes,
etc. Of course, setting these questions aside I believe that a separate crate
with backend logic will also just make the code a bit easier to maintain and
understand.

## What is Here
As I mentioned above, this crate defines an abstraction for tables. Some
highlights:

* Tables have "flat" schemas with a fixed number of columns. A prefix of those
  columns are the _primary keys_ for the table. There can be at most one row in
  the table per primary key value.

* Tables have "versions" that can be used to track growth and updates of tables
  over time. This is important because the query engine can caches `Subset`s of
  a table: the versioning system allows the system to refresh these caches when
  they become stale, or invalid.

* Tables can buffer updates without having them take effect, if applying those
  updates could cause existing `Subset`s to become invalidated. The way this is
  enforced is via the separation of `stage` and `merge` methods: `stage` methods
  move data into a table, but changes are only guaranteed to propagate after a
  `merge` takes place.

* Tables also need to implement scanning primitives, including ones that
  evaluate simple predicates on rows. Tables can optionally communicate that
  some constraints can be computed extremely quickly, which allows them to be
  evaluated without an index. This is how egglog table "timestamps" are
  supported (the query engine itself doesn't know about seminaive).

There are a lot of details needed to get all of this to work, but the upshot is
an implementation of free join that _doesn't care_ whether it is reading from a
standard egglog table or whether it is reading from a union-find. Users can even
define new tables outside of this crate and then query them the same way. The
implementation also allows for egglog's encoding of seminaive evaluation to be
written _on top_ of the existing backend (without any loss in performance).

### Free Join
This crate implements Free Join, a faster version of GJ with better batching.
Unlike the original paper, this implemention generates a plan from scratch using
a similar method to egglog, but one that tries to minimize both the _size_ of
each stage but also the _number_ of stages.

There is a lot more to do here to improve the efficiency of free join for
egglog-style queries, in particular:

* Techniques for dynamically changing the variable order.
* Techniques for dynamically "early-evaluating" stages that are lower down,
  effectively making the plan wider/bushier based on how much different stages
  overlap.

### Optimizations
Aside from free join, this crate includes a number of standard optimizations:

* Vectorized execution (both for actions and for free join).
* Memory pooling

Essentially every operation now has access to a large batch of (small, interned)
values to operate on, which could make it doable to start integrating SIMD more
directly into egglog's implementation.

### Basic Egglog
For a brief demo of what it looks like to write egglog using this backend, see
the `ac`  test in `src/tests.rs`. That test demonstrates how to implement
rebuilding and seminaive evaluation on top of the framework in this crate. Note
that we have multiple non-primary-key columns for egglog functions: one holds
the union-find id, and one holds the timestamp for the row.

## Performance
The only benchmark I have right now is a basic AC benchmark: for some number
`N`, reassociate the expression: `(+ (+ (+ ... (+ 0 1) ...) N-2) N-1)` to `(+ 0
(+ ... (+ N-2 N-1) ...))` using the rules for associativity and commutativity of
addition. For _N=12_ egglog takes 27.2s on my machine (M1 Mac) and this crate
takes 18s. For _N=13_ egglog takes 132.95s and this crate takes 82.35s.

In both cases, the new backend is 35-40% faster! Of course, there are a lot
more benchmarks to do.

## Gaps
I think there are a few more issues to sort out before we can migrate egglog to
this backend.

### Semantic Issues
We have limited support for merge functions in this crate. The
`SortedWritesTabe` allows for an arbitrary rust function to resolve conflicting
insertions, but that function can only mutate the database in limited ways:
* It can increment counters (e.g. one associated with a union-find id).
* It can stage certain values for insertion or removal (in other tables).
Actions in this crate also have support for generating a fresh id for a row if
it isn't currently present in the databse.

This is sufficient for generating arbitrary terms on the right-hand side of a
rule, but it is _not_ sufficient for observing the result of arbitrary egglog
subcomputations within an action: in other words, merge functions may "run
immediately."

This is a semantic change for egglog, but one that basically everyone I talk to
thinks is a good idea. The last time I looked at the egglog test suite, most
programs do not use this feature, and if they did we can rewrite queries
automatically to get these semantics back.

### Proofs?
My hope for proofs is that we can define an additional non-primary key that
points to the root of the _proof tree_ associated with that term. Then, as
before, each rule can propagate provenance for any new rows it adds by adding a
proof node and then inserting it into the new row. Because rebuilding and
congruence will be "just another rule", we can also add proofs there.

This doesn't make proof production trivial, but it gives us more freedom in
using native union-finds, and allowing for more information to be stored inline
rather than "a join or two away".

## Parallelism

This branch has some work towards supporting parallel egglog execution.
Essentially all the work for parallelism happens in `core-relations`: We don't
want to change the execution model to start with, we just want it so that when
you run a bunch of rules, those rules run in parallel as much as possible.


At a high level, the main architectural changes to support parallelism are as
follows:

1. All of the `stage_insert` and `stage_remove` calls are moved to a
`MutationBuffer` structure which batches mutations in a thread-local buffer
before flushing them at destruction time. `MutationBuffer` uses a thread-safe
queue under the hood to allow for batches to be accumulated and flushed in
parallel.

2. Core data-structures are now thread-safe. This is a mix of replacing hashmaps
with concurrent variants (as in the "Predicted Values" structure), and writing
our own (Union Find, Indexes [to an extent], Intern tables). Custom concurrent
data-structures are in the `concurrency` crate.

3. All main types are now marked as `Send` and `Sync`. This required refactoring
the Primitives module.

With all that, I'd say we're ready to really start experimenting with
parallelism. A few places to start:

### Parallel Rules
When running a set of rules, we currently have the following loop in the
`Database::run_rule_set` in `core-relations/src/free_join/execute.rs`:

```rust
        let mut join_state = JoinState {
            db: self,
            preds: &preds,
            subsets: Default::default(),
            bindings: Default::default(),
            var_batches: Default::default(),
            index_cache: Default::default(),
        };
        for (plan, desc) in &rule_set.plans {
            with_pool_set(|ps| {
                let start = Instant::now();
                for (id, info) in plan.atoms.iter() {
                    let table = join_state.db.get_table(info.table);
                    join_state.subsets.insert(id, table.all());
                }
                join_state.run_plan(plan, rule_set, 0, ps);
                join_state.subsets.clear();
                join_state.bindings.clear();
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    log::debug!("Rule {desc} took {elapsed:?}");
                    log::debug!("Plan for {desc}: {plan:#?}");
                } else {
                    log::trace!("Rule {desc} took {elapsed:?}");
                }
            });
        }
        let mut batches = join_state.var_batches;
        let mut exec_state = ExecutionState {
            db: self.read_only_view(),
            predicted: &preds,
            buffers: Default::default(),
        };
        // Run any remaining batches.
        for (action, ActionState { bindings, .. }) in batches.iter_mut() {
            exec_state.run_instrs(&rule_set.actions[action], bindings);
        }
```

I think we could probably move the `JoinState` construction and cleanup _inside_
the for loop and then change the loop to a rayon parallel for-each? We may have
to clean more stuff up by doing that, but it is a reasonable place to start I
think.


Once that's done, we can parallelize _within_ the recusrive GJ loop.

#### Parallelizing FJ/GJ
The core join logic happens in `JoinState::run_plan` in
`src/free_join/execute.rs`. Unlike egglog, we attempt to batch execution at a
single level before making a recursive call. Batches are flushed to a recursive
call in the `drain_updates` macro:

```rust
macro_rules! drain_updates {
    ($updates:expr) => {
        for mut update in $updates.drain(..) {
            for (var, val) in update.bindings.drain(..) {
                self.bindings.insert(var, val);
            }
            for (atom, subset) in update.refinements.drain(..) {
                self.subsets.insert(atom, subset);
            }
            self.run_plan(plan, rule_set, cur + 1, ps);
        }
    };
}
```

The outer method accumulates a number of variable bindings or refined subsets,
then we do a recursive call in a batch. I think this is a good place to kick off
a recurive call (using `rayon::scope` and `spawn`). We'll want to move
`bindings` and `refinements` out of `JoinState` and instead pass them as
parameters to `run_plan`. Then, a new scope 
could take ownership of `$updates` and copy `subsets` and `bindings` , then run
the rest of the loop as before.

We can then vary the "morsel size" here by changing the `CHUNK_SIZE` constant.

### Inter-table Write Parallelism
All writes to a table happen in its `merge` method. `merge` happens in a
fixed-point loop in the `merge_all` method in `src/free_join/mod.rs`:

```rust
pub fn merge_all(&mut self) -> bool {
    let mut ever_changed = false;
    loop {
        let mut changed = false;
        let predicted = PredictedVals::default();
        for id in 0..self.tables.n_ids() {
            // Move the table out of the tables map. Run the merge (which can
            // access the rest of the db). Then put it back.
            let table = TableId::from_usize(id);
            let mut info = self.tables.take(table);
            let table_changed = info.table.merge(&mut ExecutionState {
                predicted: &predicted,
                db: self.read_only_view(),
                buffers: Default::default(),
            });
            changed |= table_changed;
            self.tables.insert(table, info);
        }
        ever_changed |= changed;
        if !changed {
            break;
        }
    }
    ever_changed
}
```

This loop is not straightforward to parallelize in its current state, because we
remove each table from the database and then run that table's `merge` with
access to the rest of the database (that's what `read_only_view()` is doing).

This is a general and interesting problem: `merge` functions can depend on other
tables: in particular table merge functions can stage `union`s which are a table
write. I think we have two options here:

1. Move _all_ tables into a separate `DenseIdMap<TableId, ReadOptimizedLock<TableInfo>>`:
a table runs its merge function with an exclusive lock on its table, and then
references to other tables acquire a shared lock. This will cause recursive
table dependencies to deadlock (these panic in egglog today). Legal, acyclic
dependencies will naturally resolve themselves as later tables in the dependency
graph block waiting for earlier ones to finish.

2. Tables explicitly declare their merge dependencies at construction time. We
then run merge in topological order, where tables that do not depend on one
another can still be merged in parallel.

I think that option (2) is probably the better one here, though it may take a
bit more time to code up.

### Intra-table Write Parallelism
Parallelizing the outer `merge` loop may not be sufficient to get good write
parallelism. We may not have that many tables, and even if we do a small number
of tables may get most of the write load. To address this, we will want to
perform the writes themselves in parallel. This involves looking at all the
kinds of tables and parallelizing their `merge` method:

#### Parallelizing `SortedWritesTable::merge`
`SortedWritesTable` is a generalized variant of the main table data-structure in
egglog. It's a raw `HashTable` whose entries are stored in a flat array
optionally sorted by some timestamp column. To parallelize writes we need to
solve the problem of parallel hashtable updates and parallel updates to the
backing vector. Finally, we will also want to parallelize compaction of the
array. A few notes on how to do this:

* Shard the hash table _N_ ways, where _N_ is proportional to the available
  parallelism.
* Have one queue of updates per shard, rather than a global queue for the whole table.
* In parallel, drain each queue and write out a vector (per shard) of writes
  that will have to happen to the global vector.
* Resize the vector to account for the number of additional writes needed then
  write to the end of that vector in parallel. (This may require some degree of
  unsafe code)
* Now that we have offsets for new values, go back to each shard (in parallel)
  and remap the table entries to point to their new entries:
  - We can also update staleness markers here. This may _also_ require some
    amount of unsafe code, unless we want to store staleness bits in a separate
    bitset.
* Finally, we may need to compact. I think we can do this using rayon parallel
  iterators.
  - create an vector of the size of the current backing vector (the "mapping"),
    with all entries pointing to `AtomicU32::max_value`
  - iterate over the array of (row, stale) pairs.
  - enumerate
  - filter by !stale. This gives an iterator of (old offset, row) pairs.
  - collect into two vectors [offset vec, new vals vec]
  - Now build the reverse mapping in parallel by doing a parallel iterator over
    offset vec and enumerating. That gives an iterator over (new_offset, old_offset). We can then do
    `mapping[old_offset].set(new_offset)`.
  - Now we can iterate over the hash maps in parallel and then rewrite all their
    entries to point to the new offset, using `mapping`.

#### Parallelizing `DisplacedTable::merge`
`DisplacedTabe` is the table storing updates to the union-find. Unlike
`SortedWritesTable` it's possible that we could get away with doing these
updates in a single-threaded way. (Doing so could actually speed up reads on the
table).

If we _did_ want to parallelize this I think we could:

* Iterate over the updates in parallel, call `union` on the concurrent UF
  data-structure, returning a new parent new child, and timestamp if the union
  changed the UF.
* Then we could write out the updates to a new vector and sort that vector to
  get a coherent order for the unions. (Note that we always pick the min value
  when unioning)
* At that point we may want to copy the updates and update the `lookup_table`
  map in a single-threaded way. I suspect there is a way to parallelize this
  final step but it may not be worth it.

#### Parallelizing `DisplacedTableWithProvenance::merge`
`DisplacedTableWithProvenance` is that variant of the union-find that runs when
proofs are enabled. I think we would want to parallelize this last, once we have
a strategy for the underlying `DisplacedTable`. Once we have a solution there,
the only added work needed is to figure out how to update the proof graph: if we
choose not to parallelize updates to the displaced vector or the lookup table,
we can also update the proof graph in a single thread. If not, we need a similar
strategy for the graph

### Increasing Rebuild Parallelism
Once we have parallel rule execution, we should refactor the rebuild loop in
`egglog-bridge` to run rules in larger batches. Right now, rebuilding works as
follows (see `EGraph::rebuild` in `egglog-bridge/src/lib.rs` for the actual
code):

```
do:
  for table in tables:
    if incremental-rebuild-heuristic(table):
      for col in table.schema:
        db.run(table.incremental-rebuild-rule[col]);
    else:
      db.run(table.nonincremental-rebuild-rule)
until fixed point
```

We run the incremental rebuild rules one after another to avoid rebuilding a row
twice. If we can prarallelize across rules, we should probably do the follwing
instead:

```
do:
  nonincremental-rules = []
  incremental-rules = {}
  for table in tables:
    if incremental-rebuild-heuristic(table):
      for col in table.schema:
        incremental-rules[col].append(table.incremental-rebuild-rule[col])
    else:
      nonincremental-rules.append(table.nonincremental-rebuild-rule)
  db.run(nonincremental-rules + incremental-rules[0])
  for (col, rules) in ((x, y) in incremental-rules if x != 0):
    db.run(rules)
until fixed point
```
This allows us to run more rules at once, in fewer parallel rounds.

#### A custom rebuilding algorithm?
If we want to get better rebuild performance, I think we could hand-write our
own version as part of `SortedWritesTable::merge`, but I think we should try the
above strategy first as it would be less effort overall.

### Misc

* Storing the `id` counter in a single `AtomicUsize` is probably going to cause
  contention. Do we want to do a Silo-style prefecting of ids here?
* We should add calls to `rayon::yield` in `ReadOptimizedLock` to do other work
  before blocking.

### Performance / Measuring Speedups

The changes made to facilitate parallelism here can slow down single-threaded
execution. If you are interested in measuring that slowdown, check out the
initial commit on this branch.

To measure performance, time the examples in the `egglog-bridge` crate. You can
run these commands from the crate root.

```
$ cargo build --release --example ac
$ time target/release/examples/ac
$ cargo build --release --example math
$ time target/release/examples/math
```
