//! Support for egglog containers.
//!
//! Containers in egglog provide access to data-structures like vectors and sets to egglog
//! programs. While egglog can express classic functional data-structures in a straightforward way,
//! many users find it more convenient to use built-in container structures.
//!
//! The core idea behind container support to egglog is as follows:
//! - Containers are backed by a straightforward implementation in Rust. For now these are backed
//!   by standard library containers, but we may want to extend them to persistent data-structures in
//!   the future.
//! - Containers are "addressed" by a unique id that can be used for equality comparisons in egglog
//!   queries. To support this, we essentially need to keep track of all containers that are
//!   mentioned in a given egglog database.
//! - Containers need to support rebuilding. When rebuilding causes two separate ids to now point
//!   to the same container, the two ids are unioned as in a standard egglog function.
//! - Containers also export domain-specific operations that can be used to query and create
//!   data-structures.
//!
//! In this crate, we specify an interface for different containers and also provide
//! implementations for them for common data-structures used in egglog.

use core_relations::{CounterId, ExecutionState, TableId, Value};
use numeric_id::NumericId;

use std::hash::Hash;
use with_hash::WithHash;

pub(crate) mod container_env;
pub(crate) mod with_hash;

pub use container_env::ContainerEnv;

/// A specification for a container of egglog values.
pub trait Container: Hash + Eq + Send + Sync {
    /// The name of this particular container.
    fn name() -> &'static str;
    /// Create a container from a list of values.
    fn from_vals(vals: &[Value]) -> Self;
    /// Dump the contents of the container. You should always get
    /// `C == C::from_vals(&Vec::from_iter(C::contents()))`.
    fn contents(&self) -> impl Iterator<Item = Value>;
    /// Rewrite the given container according to `remap`, returning whether the contents of the
    /// container changed.
    fn rebuild(&mut self, remap: impl FnMut(Value) -> Value) -> bool;
    /// The primitive operations that can be performed on this container.
    fn primitive_ops() -> Vec<PrimitiveOperation<Self>>
    where
        Self: Sized;
}

// TODOs:
// * Hashmap => Dashmap, immutable methods everywhere for container_env
// * Wrap container_env in a struct containing non-state portions of IdGenerator_ Call that ContainerTable
// * Implement IdGenerator for DisplacedTable and DisplacedTableWithProvenance. Use that to implement merge.
// * Now to box primitives:
//   - We want a function that takes a &mut Database, + the contents of
//   IdGenerator then returns (TableId , Map<String, ExternalFunctionId>)
// * Timestamps:
//   - Wrapper should record the "next timestamp it expects", starting at 0.
//   - Can read since timestamp column in displaced table. Pick the biggest one
//   you see and increment for the next one.

/// EGraph-relevant information needed to wire up a container implementation to
/// an egglog databse.
#[derive(Copy, Clone)]
pub struct EGraphInfo {
    pub uf_table: TableId,
    pub id_counter: CounterId,
    pub proofs: bool,
}

pub struct IdGenerator_<'a, 'outer> {
    state: &'a mut ExecutionState<'outer>,
    uf_table: TableId,
    id_counter: CounterId,
    next_ts: Value,
    proofs: bool,
}

impl IdGenerator_<'_, '_> {
    fn generate_id(&self) -> Value {
        Value::from_usize(self.state.inc_counter(self.id_counter))
    }
}

/// The subset of the egglog database needed to support containers. Operations are immutable to
/// make it easier to parallelize container management.
pub trait UnionFindHandle: Send {
    fn new_handle(&self) -> Self;
    fn generate_id(&self) -> Value;
    fn union(&self, id1: Value, id2: Value) -> Value;
    fn find(&self, id: Value) -> Value;
}

/// A UnionFindHandle that resolves new ids by looking them up in a table, via
/// dynamic dispatch.
///
/// This is a fairly straightforward handle that is used in implementing
/// external functions. It does no batching to avoid the overhead of virtual
/// method dispatch.
pub struct DynamicUnionFindHandle<'outer> {
    info: EGraphInfo,
    state: ExecutionState<'outer>,
    next_ts: Value,
}

// TODO/question: how do we get the next timestamp?
// Probably need to pass it into the dynamicUF.

impl UnionFindHandle for DynamicUnionFindHandle<'_> {
    fn new_handle(&self) -> Self {
        Self {
            info: self.info,
            next_ts: self.next_ts,
            state: self.state.new_handle(),
        }
    }

    fn generate_id(&self) -> Value {
        Value::from_usize(self.state.inc_counter(self.info.id_counter))
    }

    fn union(&self, id1: Value, id2: Value) -> Value {
        todo!()
    }

    fn find(&self, id: Value) -> Value {
        todo!()
    }
}

/// A primitive operation on a container type.
pub struct PrimitiveOperation<C> {
    /// The name of the primitive operation.
    pub name: String,
    /// The operation itself, run with respect to a ContainerEnv.
    #[allow(clippy::type_complexity)]
    pub operation:
        Box<dyn Fn(&ContainerEnv<C>, &mut ExecutionState, &[Value]) -> Option<Value> + Send + Sync>,
}

impl Container for WithHash<Vec<Value>> {
    fn name() -> &'static str {
        "vec"
    }

    fn from_vals(vals: &[Value]) -> Self {
        vals.to_vec().into()
    }

    fn contents(&self) -> impl Iterator<Item = Value> {
        self.iter().copied()
    }

    fn rebuild(&mut self, mut remap: impl FnMut(Value) -> Value) -> bool {
        let mut changed = false;
        for val in self.iter_mut() {
            let new_val = remap(*val);
            if new_val != *val {
                changed = true;
                *val = new_val;
            }
        }
        changed
    }

    fn primitive_ops() -> Vec<PrimitiveOperation<Self>> {
        vec![PrimitiveOperation {
            name: "push".to_string(),
            operation: Box::new(
                // To solve this problem: We probably want an IdGenerator
                // implementation backed by an ExecutionState that can query the
                // UF? Or perhaps we just want to resolve it dynamically?
                |env: &ContainerEnv<Self>, state: &mut ExecutionState, args: &[Value]| {
                    let id = args[0];
                    let val = args[1];
                    let container = env.get_container(id)?;
                    let mut container = container.clone();
                    container.push(val);
                    todo!()
                    // Some(env.get_id(container))
                },
            ),
        }]
        //     PrimitiveOperation {
        //         name: "pop".to_string(),
        //         operation: Box::new(|env, args| {
        //             let id = args[0];
        //             let container = env.get_container(id)?;
        //             let mut container = container.clone();
        //             container.pop();
        //             Some(id)
        //         }),
        //     },
        // ]
    }
}
