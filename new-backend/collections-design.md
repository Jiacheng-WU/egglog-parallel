# design for collections in the new backend


## How vectors Work

* Create a table that can effectively "listen" for merge values, but has its
  own set of internal APIs.
* Have an `ExternalFunction` for `vec-of` `vec-get` and `vec-set`

Ideally we'd have an architecture mirroring the rest of the setup, where
`vec-of` and `vec-set` can be mostly-local and `vec-get` can be highly
scalable.

Even better would be to have efficient parallel merges.

### One sketch

* Sharded Hash Table mapping `Vec<Value> => Id` [T1]
* Sharded Hash Table mapping `Id => Vec<Value>` [T2]
* Maybe there's some sort of backing store for these vectors? [Start with
  Arc<[Value]>, or (hashcode, Arc<[Value]>)]
* `vec-get` does a normal read of T2
* `vec-set` does a normal read of T2, then a blind write into a local table.
    * Local tables... how do those work for ExternalFunctions ? 
    * New APIs for storing said state?
    * Can we use prediction API? Or maybe hijack get_row() (wouldn't be local
      state at that point, but we could have Predicted be specific to the
      table)
* `vec-of` basically a `vec-set` without the read.

So all of these operations are external functions.

* Merges work by first assimilating any predicted values (in parallel?) and
  then running rebuilding.

*Assimilating Predicted Vals*
* Serially: Just write to T1, unioning on conflict, and the  updating T2
  accordingly.
* In parallel: Shard vals by queues for T1, drain those queues and write them
  out to queues for T2, buffering changes to UF if we find two Ids that are
  equal.

*Rebuilding*
* Incremental:
  - store timestamp of last successful rebuild. Don't record timestamps on
    vector insertion.
  - maintain index from Val to `Set<Ids of vecs that contain that Val>`
  - Just update it in lockstep, basically as a third phase for parallel writes.
  - For all ids noncanonical since last rebuild, visit all ids and
    recanonicalize.
  - Doing this in parallel seems doable but annoying.
* Nonincremental:
  - Scan T1, if mutated, update T2 accordingly, etc.

## Structure

Most of the hard part of getting vectors working is schematic and should apply
to all containers. We should build an abstract API that gives us this for free
for any container. Egglog basically does this. Something like:

```rust

trait ContainerEnv<C: Container> {
    fn get_container(&self, val: Value) -> Option<C>;
    fn box_container(&mut self, c: C) -> Value;
}

trait Container: Hash + Eq + Send + Sync {

fn from(elts: &[Vec]) -> Self;
fn rebuild(&mut self, f: impl FnMut(Value) -> Value) -> bool;
fn contents(&self) -> impl Iterator<Item=Value>;
fn primitives<E: ConainerEnv<Self>>() -> Vec<(String, Box<dyn Fn(&mut E, &[Value]) -> Option<Value>>)>;
}
```


