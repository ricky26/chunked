//! Base definitions for components.
//! 
//! All entities in this library are built out of components. There is no intrinsic
//! value to an entity. This module provides means of defining and managing
//! components.
//!
//! Each component type is allocated a unique ID. There is a macro (`component`)
//! to help you assign this unique ID.

use std::alloc::Layout;
use std::cmp::{Ord, Ordering};
use std::fmt::{Debug, Formatter};
use std::sync::{RwLock, Arc};

use once_cell::sync::{Lazy, OnceCell};

use crate::EntityID;
use std::fmt;
use std::any::type_name;

/// A component type ID which is unique for a specific component type.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComponentTypeID(usize);

struct ComponentRegistry {
    component_types: Vec<Arc<ComponentRegistration>>,
}

static mut COMPONENT_REGISTRY: Lazy<RwLock<ComponentRegistry>> = Lazy::new(|| {
    RwLock::new(ComponentRegistry {
        component_types: vec![
            Arc::new(ComponentRegistration::new::<EntityID>(ComponentTypeID(0))),
        ],
    })
});

impl ComponentTypeID {
    /// Construct a new `ComponentTypeID` from the inner value.
    pub(crate) fn new(inner: usize) -> ComponentTypeID {
        ComponentTypeID(inner)
    }

    /// Create a new globally unique `ComponentTypeID`.
    pub fn register<T: Component>() -> ComponentTypeID {
        unsafe {
            let mut r = COMPONENT_REGISTRY.write().unwrap();
            let id = ComponentTypeID(r.component_types.len());
            r.component_types.push(Arc::new(ComponentRegistration::new::<T>(id)));
            id
        }
    }

    /// Fetch the registration for this `ComponentTypeID` returning None if it is
    /// missing from the registry.
    fn safe_registration(&self) -> Option<Arc<ComponentRegistration>> {
        unsafe {
            let r = COMPONENT_REGISTRY.read().unwrap();
            r.component_types.get(self.0).cloned()
        }
    }

    /// Fetch the registration information for a component type.
    pub fn registration(&self) -> Arc<ComponentRegistration> {
        self.safe_registration().unwrap()
    }

    /// Return the inner unique ID.
    pub fn id(&self) -> usize {
        self.0
    }

    /// Fetch the memory layout of this component type.
    pub fn layout(&self) -> Layout {
        self.registration().layout()
    }

    /// Return the name of this registration.
    pub fn name(&self) -> &'static str {
        self.registration().name()
    }
}

impl Debug for ComponentTypeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.safe_registration() {
            Some(reg) => write!(f, "{}", reg.name()),
            None => write!(f, "ComponentTypeID(#{} missing)", self.0),
        }
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
    pub fn get<T: Component>(&self) -> ComponentTypeID {
        self.0.get_or_init(ComponentTypeID::register::<T>).clone()
    }
}

/// The component trait is implemented on all component types.
/// 
/// This trait is unsafe, because implementing it and not returning a unique
/// `type_id` can result in other safe functions on `Chunks` performing illegal
/// casts.
pub unsafe trait Component: Debug + Default + Copy {
    /// Get the unique type ID of this component.
    fn type_id() -> ComponentTypeID;

    /// Get the memory layout of an instance of this component.
    fn layout() -> Layout;
}

/// A ComponentRegistration is the dynamic version of a type implementing Component.
#[derive(Clone, Copy)]
pub struct ComponentRegistration {
    type_id: ComponentTypeID,
    layout: Layout,
    set_default: fn(&mut [u8]),
    name: &'static str,
}

impl ComponentRegistration {
    /// Create a ComponentRegistration for a static type.
    pub fn new<T: Component>(type_id: ComponentTypeID) -> ComponentRegistration {
        fn default<T: Component>(ptr: &mut [u8]) {
            assert_eq!(ptr.len(), std::mem::size_of::<T>());
            let ptr: &mut T = unsafe { std::mem::transmute(ptr.as_ptr()) };
            let d = T::default();
            *ptr = d;
        }

        ComponentRegistration {
            type_id,
            layout: T::layout(),
            set_default: default::<T>,
            name: type_name::<T>(),
        }
    }

    /// Return the unique type ID for this `ComponentRegistration`.
    pub fn type_id(&self) -> ComponentTypeID {
        self.type_id
    }

    /// Return the memory layout of a single instance of this component.
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Get the name of this component type.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Given the storage buffer of a component instance, fill in the default
    /// value.
    pub fn set_default(&self, ptr: &mut [u8]) {
        (self.set_default)(ptr)
    }
}

impl PartialEq for ComponentRegistration {
    fn eq(&self, other: &ComponentRegistration) -> bool {
        self.type_id.eq(&other.type_id)
    }
}

impl Eq for ComponentRegistration {}

impl PartialOrd for ComponentRegistration {
    fn partial_cmp(&self, other: &ComponentRegistration) -> Option<Ordering> {
        self.type_id.partial_cmp(&other.type_id)
    }
}

impl Ord for ComponentRegistration {
    fn cmp(&self, other: &ComponentRegistration) -> Ordering {
        self.type_id.cmp(&other.type_id)
    }
}

impl Debug for ComponentRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<ComponentRegistration {:?}>", self.type_id.id())
    }
}

/// Implement the `Component` trait on a type.
///
/// Component types must implement Copy and Default.
#[macro_export]
macro_rules! component {
    ($i:ident) => {
        const _: () = {
            static INIT_TYPE: $crate::component::AutoComponentTypeID = $crate::component::AutoComponentTypeID::new();

            unsafe impl $crate::component::Component for $i {
                fn type_id() -> $crate::component::ComponentTypeID {
                    INIT_TYPE.get::<$i>()
                }

                fn layout() -> ::core::alloc::Layout {
                    ::core::alloc::Layout::new::<$i>()
                }
            }

            ()
        };
    };
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_uniqueness() {
        #[derive(Debug, Clone, Copy, Default)]
        struct A;
        #[derive(Debug, Clone, Copy, Default)]
        struct B;

        component!(A);
        component!(B);

        assert_ne!(ComponentTypeID(0), A::type_id());
        assert_ne!(ComponentTypeID(0), B::type_id());
        assert_ne!(A::type_id(), B::type_id());
    }

    #[test]
    fn test_default() {
        #[derive(Debug, Clone, Copy)]
        struct A(u8);

        component!(A);

        impl Default for A {
            fn default() -> A {
                A(42)
            }
        }

        let component_type = ComponentRegistration::new::<A>(ComponentTypeID(12));
        assert_eq!(component_type.type_id(), A::type_id());
        assert_eq!(component_type.layout(), Layout::new::<A>());

        let raw = &mut [0];
        component_type.set_default(raw);
        assert_eq!(raw[0], 42);
    }
}