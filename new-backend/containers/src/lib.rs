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
//  * Rename IdGenerator_ to EGraphHandle
//  * Rename IdGenerator to UnionFindView
//  * Implement UnionFindView for either DisplacedTable or DisplacedTableWithProvenance
//  * Hashmap => Dashmap [probably]
//  * We still don't have the e2e flow mapped out but we're getting closer.
//    - External func should have all but state, then get state added.
//    - Maybe needs to take `state` as an arg to all methods.
//    - ... remember, everything needs to end up in the external func and the table evenetually.

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
pub trait IdGenerator: Send + Sync {
    fn generate_id(&self) -> Value;
    fn union(&self, id1: Value, id2: Value) -> Value;
    fn find(&self, id: Value) -> Value;
}

/// A primitive operation on a container type.
pub struct PrimitiveOperation<C> {
    /// The name of the primitive operation.
    pub name: String,
    /// The operation itself, run with respect to a ContainerEnv.
    #[allow(clippy::type_complexity)]
    pub operation: Box<
        dyn Fn(&mut ContainerEnv<C>, &mut ExecutionState, &[Value]) -> Option<Value> + Send + Sync,
    >,
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
        todo!()
        // vec![
        //     PrimitiveOperation {
        //         name: "push".to_string(),
        //         operation: Box::new(|env, args| {
        //             let id = args[0];
        //             let val = args[1];
        //             let container = env.get_container(id)?;
        //             let mut container = container.clone();
        //             container.push(val);
        //             Some(env.get_id(container, env))
        //         }),
        //     },
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
