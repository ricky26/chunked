//! Base Entity types.
//! 
//! All entities are built up of Components. In this crate all entities are issued
//! a unique ID (per world) and each component type is allocated a unique ID
//! (globally). There is a macro (`component`) to help you assign a unique ID
//! for component types.

use std::ops::Deref;
use std::fmt::Debug;
use std::alloc::Layout;
use std::sync::{Arc, atomic::{self, AtomicUsize}};
use std::cmp::{Ord, Ordering};
use once_cell::sync::OnceCell;

/// A component type ID which is unique for a specific component type.
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
pub struct ComponentTypeID(usize);

static mut NEXT_TYPE_ID: AtomicUsize = AtomicUsize::new(1);

impl ComponentTypeID {
    /// Construct a new `ComponentTypeID` from the inner value.
    pub fn new(inner: usize) -> ComponentTypeID {
        ComponentTypeID(inner)
    }

    /// Create a new globally unique `ComponentTypeID`.
    pub fn unique() -> ComponentTypeID {
        let id = unsafe { NEXT_TYPE_ID.fetch_add(1, atomic::Ordering::Relaxed) };
        ComponentTypeID(id)
    }

    /// Return the inner unique ID.
    pub fn id(&self) -> usize {
        self.0
    }
}

/// A struct for lazily assigning unique `ComponentTypeID`s.
pub struct AutoComponentTypeID(OnceCell<ComponentTypeID>);

impl AutoComponentTypeID {
    /// Create a new `AutoComponentTypeID`.
    pub const fn new() -> AutoComponentTypeID {
        AutoComponentTypeID(OnceCell::new())
    }

    /// Get the `ComponentTypeID` this struct wraps.
    pub fn get(&self) -> ComponentTypeID {
        self.0.get_or_init(|| ComponentTypeID::unique()).clone()
    }
}

/// The component trait is implemented on all component types.
/// 
/// This trait is unsafe, because implementing it and not returning a unique
/// `type_id` can result in other safe functions on `Chunks` performing illegal
/// casts.
pub unsafe trait Component: Default + Copy {
    /// Get the unique type ID of this component.
    fn type_id() -> ComponentTypeID;

    /// Get the memory layout of an instance of this component.
    fn layout() -> Layout;
}

/// Implement the `Component` trait on a type.
/// 
/// Component types must implement Copy and Default.
#[macro_export]
macro_rules! component {
    ($i:ident) => {
        const _: () = {
            static INIT_TYPE: ::ecs::entity::AutoComponentTypeID = ::ecs::entity::AutoComponentTypeID::new();

            unsafe impl ::ecs::Component for $i {
                fn type_id() -> ::ecs::ComponentTypeID {
                    INIT_TYPE.get()
                }

                fn layout() -> ::core::alloc::Layout {
                    ::core::alloc::Layout::new::<$i>()
                }
            }

            ()
        };
    };
}

/// A ComponentType is the dynamic version of a type implementing Component.
#[derive(Clone,Copy)]
pub struct ComponentType {
    type_id: ComponentTypeID,
    layout: Layout,
    set_default: fn(&mut [u8]),
}

impl ComponentType {
    /// Create a ComponentType for a static type.
    pub fn for_type<T: Component>() -> ComponentType {
        fn default<T: Component>(ptr: &mut [u8]) {
            assert!(ptr.len() == std::mem::size_of::<T>());
            let ptr = unsafe { std::mem::transmute(ptr.as_ptr()) };
            let d = T::default();
            std::mem::replace(ptr, d);
        }

        ComponentType{
            type_id: T::type_id(),
            layout: T::layout(),
            set_default: default::<T>,
        }
    }

    /// Return the unique type ID for this `ComponentType`.
    pub fn type_id(&self) -> ComponentTypeID {
        self.type_id
    }

    /// Return the memory layout of a single instance of this component.
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Given the storage buffer of a component instance, fill in the default
    /// value.
    pub fn set_default(&self, ptr: &mut [u8]) {
        (self.set_default)(ptr)
    }
}

impl PartialEq for ComponentType {
    fn eq(&self, other: &ComponentType) -> bool {
        self.type_id.eq(&other.type_id)
    }
}

impl Eq for ComponentType {}

impl PartialOrd for ComponentType {
    fn partial_cmp(&self, other: &ComponentType) -> Option<Ordering> {
        self.type_id.partial_cmp(&other.type_id)
    }
}

impl Ord for ComponentType {
    fn cmp(&self, other: &ComponentType) -> Ordering {
        self.type_id.cmp(&other.type_id)
    }
}

/// The trait used to convey raw component data.
pub unsafe trait ComponentSource {
    /// Given the storage for a given component, set the value from the
    /// `ComponentStorage`.
    fn set_component(&self, component_type: &ComponentType, storage: &mut [u8]);
}

/// A `ComponentSource` which just calls out to `Default::default`.
/// 
/// This is used to initialise entities to a fully default state.
pub struct DefaultComponentSource;

unsafe impl ComponentSource for DefaultComponentSource {
    fn set_component(&self, component_type: &ComponentType, storage: &mut [u8]) {
        component_type.set_default(storage);
    }
}

/// An archetype represents a particular layout of an entity.
/// 
/// It contains a set of `ComponentType`s which are sorted for convenience.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Archetype {
    component_types: Vec<ComponentType>,
}

impl Archetype {
    /// Create a new archetype given the component set.
    /// 
    /// `component_types` should be sorted.
    pub fn new(component_types: Vec<ComponentType>) -> Archetype {
        Archetype{
            component_types: component_types,
        }
    }

    /// Return the sorted list of component types in this archetype.
    pub fn component_types(&self) -> &[ComponentType] {
        &self.component_types
    }

    /// Returns true if this archetype contains the given component.
    pub fn has_component_type(&self, component_type: &ComponentType) -> bool {
        self.component_types.binary_search(component_type).is_ok()
    }

    /// Returns true if this archetype contains all of the given component types.
    pub fn has_all_component_types<T: Deref<Target=ComponentType>>(&self, component_types: impl IntoIterator<Item=T>) -> bool {
        component_types.into_iter().all(|ct| self.has_component_type(&*ct))
    }
}

/// An iterator over components of a single type contained in an archetype.
pub struct EntityComponentIterator {
    archetype: Option<Arc<Archetype>>,
    component_index: usize,
}

impl EntityComponentIterator {
    /// Create a new empty `EntityComponentIterator`.
    pub fn empty() -> EntityComponentIterator {
        EntityComponentIterator{
            archetype: None,
            component_index: 0,
        }
    }

    /// Create an `EntityComponentIterator` for a given type.
    pub fn for_archetype(archetype: Arc<Archetype>) -> EntityComponentIterator {
        EntityComponentIterator{
            archetype: Some(archetype),
            component_index: 0,
        }
    }
}

impl Iterator for EntityComponentIterator {
    type Item = ComponentTypeID;

    fn next(&mut self) -> Option<ComponentTypeID> {
        if let Some(archetype) = &self.archetype {
            let types = &archetype.component_types;
            if self.component_index >= types.len() {
                None
            } else {
                let type_id = types[self.component_index].type_id;
                self.component_index += 1;
                Some(type_id)
            }
        } else {
            None
        }
    }
}

/// The ID of a single entity.
/// 
/// Entity IDs are unique per World. They are not unique across worlds.
#[derive(Debug,Clone,Copy,Default,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct EntityID(usize);

unsafe impl Component for EntityID {
    fn type_id() -> ComponentTypeID {
        ComponentTypeID::new(0)
    }

    fn layout() -> Layout {
        Layout::new::<Self>()
    }
}

impl EntityID {
    /// Create a new EntityID given the inner unique ID.
    pub fn new(id: usize) -> EntityID {
        EntityID(id)
    }

    /// Return the inner unique ID.
    pub fn id(&self) -> usize {
        self.0
    }
}
