//! A simple data-structure for tracking the dependencies of the merge functions
//! from different tables on one another.

use numeric_id::{define_id, DenseIdMap, NumericId};

use crate::{common::IndexSet, TableId};

define_id!(
    LevelId,
    u32,
    "an identifier for a level in the dependency graph"
);

#[derive(Default)]
pub(crate) struct DependencyGraph {
    levels: DenseIdMap<LevelId, IndexSet<TableId>>,
    to_level: DenseIdMap<TableId, LevelId>,
}

impl DependencyGraph {
    pub(crate) fn add_table(&mut self, table: TableId, deps: impl IntoIterator<Item = TableId>) {
        assert!(
            self.to_level.get(table).is_none(),
            "table {table:?} already added to graph"
        );
        let level = match deps
            .into_iter()
            .map(|dep| *self.to_level.get(dep).unwrap())
            .max()
        {
            Some(level) => level.inc(),
            None => LevelId::new(0),
        };
        self.to_level.insert(table, level);
        self.levels.get_or_default(level).insert(table);
    }

    pub(crate) fn strata(&self) -> impl Iterator<Item = &IndexSet<TableId>> {
        self.levels.iter().map(|(_, tables)| tables)
    }
}
