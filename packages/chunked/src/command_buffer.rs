use std::collections::{btree_map, BTreeMap};

use crate::component::{Component, ComponentTypeID};
use crate::component_data::ComponentValueRef;
use crate::entity::EntityID;
use crate::snapshot::{Edit, EditAction};

#[derive(Clone, Debug)]
enum ComponentAction {
    Set(usize, usize),
    Remove,
}

/// A command buffer for entity changes.
///
/// This is used as an allocated list of `Snapshot` modifications.
/// The primary benefit is that this can be kept around between snapshot
/// modifications and applied later. With `CommandBuffer`s multiple systems can
/// determine changes in parallel and apply them at the end.
#[derive(Clone, Debug)]
pub struct CommandBuffer {
    component_data: Vec<u8>,
    commands: BTreeMap<(EntityID, ComponentTypeID), ComponentAction>,
}

impl CommandBuffer {
    /// Create a new, empty, command buffer.
    pub fn new() -> CommandBuffer {
        CommandBuffer {
            component_data: Vec::new(),
            commands: BTreeMap::new(),
        }
    }

    /// Merge multiple command buffers.
    ///
    /// If multiple command buffers reference the same component, the rightmost wins.
    pub fn merge(buffers: &[&CommandBuffer]) -> CommandBuffer {
        let mut dest = CommandBuffer::new();
        let max_data = buffers.iter()
            .fold(0, |data, buffer| {
                data + buffer.component_data.len()
            });
        dest.component_data.reserve(max_data);

        let mut entity_merger = EntityMerger::new();
        let mut iters = buffers.iter()
            .map(|b| b.commands.iter().peekable())
            .collect::<Vec<_>>();

        loop {
            let next_entity = iters.iter_mut()
                .filter_map(|i| i.peek().map(|e| e.0.0))
                .min();
            if next_entity.is_none() {
                break;
            }

            let next_entity = next_entity.unwrap();

            for (buffer_idx, i) in iters.iter_mut().enumerate() {
                let ok = i.peek().map_or(false, |x| x.0.0 == next_entity);
                if !ok {
                    continue;
                }

                while i.peek().map_or(false, |x| x.0.0 == next_entity) {
                    let ((_, type_id), action) = i.next().unwrap();

                    let new_action = match action {
                        ComponentAction::Set(start, end) =>
                            EntityMergeAction::Set(buffer_idx, *start, *end),
                        ComponentAction::Remove => EntityMergeAction::Remove,
                    };
                    entity_merger.insert(*type_id, new_action);
                }
            }

            // If there are no entries, we're not actually moving any of the iterators forward,
            // which means we'll loop forever.
            assert!(!entity_merger.entries.is_empty());
            for (type_id, action) in entity_merger.entries.iter() {
                let new_action = match action {
                    EntityMergeAction::Set(buffer, start, end) => {
                        let offset = dest.component_data.len();
                        dest.component_data.extend_from_slice(
                            &buffers[*buffer].component_data[*start..*end]);
                        ComponentAction::Set(offset, offset + end - start)
                    }
                    EntityMergeAction::Remove => ComponentAction::Remove
                };
                dest.commands.insert((next_entity, *type_id), new_action);
            }
            entity_merger.clear();
        }

        dest
    }

    /// Iterate over the edit list for this command buffer.
    ///
    /// This is usually used with `Snapshot::modify`.
    pub fn iter_edits(&self) -> CommandBufferIterator<'_> {
        CommandBufferIterator::new(self)
    }

    /// Set a single component on an entity.
    pub fn set_component<T: Component>(&mut self, entity_id: EntityID, component: &T) {
        let v = ComponentValueRef::from(component);
        let data = v.as_slice();

        let start = self.component_data.len();
        let end = start + data.len();
        self.component_data.extend_from_slice(data);
        self.commands.insert(
            (entity_id, v.type_id()),
            ComponentAction::Set(start, end));
    }

    /// Remove a single component from an entity.
    pub fn remove_component<T: Component>(&mut self, entity_id: EntityID) {
        self.commands.insert((entity_id, T::type_id()), ComponentAction::Remove);
    }
}

/// An iterator over the edit list of a command buffer.
pub struct CommandBufferIterator<'a> {
    buffer: &'a CommandBuffer,
    iter: btree_map::Iter<'a, (EntityID, ComponentTypeID), ComponentAction>,
}

impl<'a> CommandBufferIterator<'a> {
    fn new(buffer: &'a CommandBuffer) -> CommandBufferIterator<'a> {
        let iter = buffer.commands.iter();
        CommandBufferIterator {
            buffer,
            iter,
        }
    }
}

impl<'a> Iterator for CommandBufferIterator<'a> {
    type Item = Edit<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(((id, type_id), command)) => {
                let action = match command {
                    ComponentAction::Set(start, end) => unsafe {
                        EditAction::SetComponent(ComponentValueRef::from_raw(
                            *type_id,
                            &self.buffer.component_data[*start..*end],
                        ))
                    },
                    ComponentAction::Remove => EditAction::RemoveComponent(*type_id),
                };

                Some(Edit(*id, action))
            }
        }
    }
}

enum EntityMergeAction {
    Set(usize, usize, usize),
    Remove,
}

/// A utility for merging multiple sets of entity edits.
struct EntityMerger {
    entries: Vec<(ComponentTypeID, EntityMergeAction)>,
}

impl EntityMerger {
    /// Create a new entity merger.
    pub fn new() -> EntityMerger {
        EntityMerger {
            entries: Vec::new(),
        }
    }

    /// Clear the merger.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Insert an action.
    pub fn insert(&mut self, type_id: ComponentTypeID, action: EntityMergeAction) {
        match self.entries.binary_search_by_key(&type_id, |e| e.0) {
            Ok(idx) => { self.entries[idx].1 = action; }
            Err(idx) => { self.entries.insert(idx, (type_id, action)); }
        }
    }
}
