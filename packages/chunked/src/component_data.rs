//! Containers for component data.
//!
//! Usually used for updating entities in the system individually.

use std::convert::TryFrom;

use crate::component::{Component, ComponentTypeID};

/// A safe reference to a component value.
#[derive(Debug, Clone)]
pub struct ComponentValueRef<'a> {
    type_id: ComponentTypeID,
    slice: &'a [u8],
}

impl<'a> ComponentValueRef<'a> {
    /// Create a `ComponentValueRef` from a raw byte array.
    pub unsafe fn from_raw(type_id: ComponentTypeID, data: &'a [u8]) -> ComponentValueRef<'a> {
        ComponentValueRef {
            type_id,
            slice: data,
        }
    }

    /// Get the type ID of the stored component value reference.
    pub fn type_id(&self) -> ComponentTypeID {
        self.type_id
    }

    /// Return the raw bytes of the component.
    pub fn as_slice(&self) -> &'a [u8] {
        self.slice
    }

    /// Attempt to downcast this value ref back to a reference to the component type.
    pub fn downcast<T: Component>(&self) -> Option<&T> {
        if T::type_id() == self.type_id {
            Some(unsafe { std::mem::transmute(self.slice.as_ptr()) })
        } else {
            None
        }
    }
}

impl<'a, T: Component> From<&T> for ComponentValueRef<'a> {
    fn from(v: &T) -> Self {
        let slice = unsafe {
            let ptr = v as *const _;
            std::slice::from_raw_parts(ptr as _, std::mem::size_of::<T>())
        };

        ComponentValueRef {
            type_id: T::type_id(),
            slice,
        }
    }
}

/// Component data to assign to an entity.
///
/// Invariant: this iterator must be sorted.
pub trait ComponentData<'a> {
    type Iterator: Iterator<Item=ComponentValueRef<'a>> + Clone;

    /// Return an iterator over this component data.
    ///
    /// This iterator must be sorted by type ID and contain no duplicates.
    fn iter(&self) -> Self::Iterator;
}

/// A slice of component value references.
pub struct ComponentDataSlice<'a, 'b>(pub &'b [ComponentValueRef<'a>]);

impl<'a, 'b> ComponentDataSlice<'a, 'b> {
    pub fn to_owned(&self) -> ComponentDataVec<'a> {
        ComponentDataVec(self.0.to_owned())
    }
}

impl<'a, 'b> ComponentData<'a> for ComponentDataSlice<'a, 'b> {
    type Iterator = std::iter::Cloned<std::slice::Iter<'b, ComponentValueRef<'a>>>;

    fn iter(&self) -> Self::Iterator {
        self.0.iter().cloned()
    }
}

impl<'a, 'b> TryFrom<&'b [ComponentValueRef<'a>]> for ComponentDataSlice<'a, 'b> {
    type Error = ();

    fn try_from(value: &'b [ComponentValueRef<'a>]) -> Result<Self, Self::Error> {
        for s in value.windows(2) {
            let a = &s[0];
            let b = &s[1];

            if b.type_id <= a.type_id {
                return Err(());
            }
        }

        Ok(ComponentDataSlice(value))
    }
}

impl<'a, 'b> TryFrom<&'b mut [ComponentValueRef<'a>]> for ComponentDataSlice<'a, 'b> {
    type Error = ();

    fn try_from(value: &'b mut [ComponentValueRef<'a>]) -> Result<Self, Self::Error> {
        for s in value.windows(2) {
            let a = &s[0];
            let b = &s[1];

            if b.type_id == a.type_id {
                return Err(());
            }
        }

        value.sort_by(|a, b| a.type_id.cmp(&b.type_id));
        Ok(ComponentDataSlice(value))
    }
}

/// A vector of component data.
#[derive(Debug, Clone)]
pub struct ComponentDataVec<'a>(Vec<ComponentValueRef<'a>>);

impl<'a> ComponentDataVec<'a> {
    /// Create an empty `ComponentDataVec`.
    pub fn new() -> ComponentDataVec<'a> {
        ComponentDataVec(Vec::new())
    }

    /// Return the slice contents of this `ComponentDataVec`.
    pub fn as_slice(&self) -> &[ComponentValueRef<'a>] {
        &self.0[..]
    }

    /// Return the number of entries in this mapping.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Remove all component data from this `ComponentDataVec`.
    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Set a component's value.
    pub fn set_component(&mut self, component_data: ComponentValueRef<'a>) {
        match self.0.binary_search_by_key(&component_data.type_id(), |r| r.type_id()) {
            Ok(idx) => self.0[idx] = component_data,
            Err(idx) => self.0.insert(idx, component_data),
        }
    }

    /// Remove a component from this `ComponentDataVec`.
    pub fn remove_component(&mut self, type_id: ComponentTypeID) {
        match self.0.binary_search_by_key(&type_id, |r| r.type_id()) {
            Ok(idx) => { self.0.remove(idx); }
            Err(_) => {}
        };
    }
}

impl<'a> ComponentData<'a> for ComponentDataVec<'a> {
    type Iterator = std::iter::Cloned<std::slice::Iter<'a, ComponentValueRef<'a>>>;

    fn iter(&self) -> Self::Iterator {
        unsafe { std::mem::transmute(self.0.iter().cloned()) }
    }
}

impl<'a> TryFrom<Vec<ComponentValueRef<'a>>> for ComponentDataVec<'a> {
    type Error = ();

    fn try_from(mut value: Vec<ComponentValueRef<'a>>) -> Result<Self, Self::Error> {
        ComponentDataSlice::try_from(&mut value[..])?;
        Ok(ComponentDataVec(value))
    }
}

/// A utility struct for writing component data.
#[derive(Debug)]
pub struct ComponentDataVecWriter<'a, 'b> {
    vec: &'b mut Vec<ComponentValueRef<'a>>,
    offset: usize,
}

impl<'a, 'b> ComponentDataVecWriter<'a, 'b> {
    /// Create a new `Vec` writer given the vector.
    pub fn new(vec: &'b mut Vec<ComponentValueRef<'a>>) -> ComponentDataVecWriter<'a, 'b> {
        let offset = vec.len();
        ComponentDataVecWriter {
            vec,
            offset,
        }
    }

    /// Return the slice contents of this `ComponentDataVec`.
    pub fn as_slice(&self) -> &[ComponentValueRef<'a>] {
        &self.vec[self.offset..]
    }

    /// Return the number of entries in this mapping.
    pub fn len(&self) -> usize {
        self.vec.len() - self.offset
    }

    /// Set a component's value.
    pub fn set_component(&mut self, component_data: ComponentValueRef<'a>) {
        let slice = &mut self.vec[self.offset..];

        match slice.binary_search_by_key(&component_data.type_id(), |r| r.type_id()) {
            Ok(idx) => slice[idx] = component_data,
            Err(idx) => self.vec.insert(self.offset + idx, component_data),
        }
    }

    /// Remove a component from this writer.
    pub fn remove_component(&mut self, type_id: ComponentTypeID) {
        let slice = &mut self.vec[self.offset..];
        match slice.binary_search_by_key(&type_id, |r| r.type_id()) {
            Ok(idx) => { self.vec.remove(self.offset + idx); }
            Err(_) => {}
        };
    }

    /// Return the range of entries in the `Vec` which this writer uses.
    pub fn range(&self) -> (usize, usize) {
        (self.offset, self.vec.len())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::component;

    #[test]
    fn test_component_data() {
        #[derive(Clone, Copy, Default, Debug)]
        struct A(u8);
        component!(A);

        #[derive(Clone, Copy, Default, Debug)]
        struct B(u8);
        component!(B);

        let value_ref = ComponentValueRef::from(&A(23));
        assert_eq!(value_ref.type_id(), A::type_id());
        assert_eq!(value_ref.as_slice()[0], 23);

        let mut unsorted_data = [
            (&B(24)).into(),
            (&A(51)).into(),
        ];
        let data = ComponentDataSlice::try_from(&mut unsorted_data[..]).unwrap();
        assert_eq!(data.0.len(), 2);
        assert_eq!(data.0[0].type_id(), A::type_id());
        assert_eq!(data.0[1].type_id(), B::type_id());
        assert_eq!(data.0[0].as_slice()[0], 51);
        assert_eq!(data.0[1].as_slice()[0], 24);
    }
}
