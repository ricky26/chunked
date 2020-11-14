//! A reusable Box.
#![allow(dead_code)]

use std::pin::Pin;
use std::ptr::{NonNull};
use std::mem::{forget};
use std::alloc::{Layout, alloc, realloc, dealloc};
use std::marker::{Unpin, PhantomData};
use std::ops::{Deref, DerefMut};
use std::future::Future;
use std::task::{Poll, Context};

/// An allocation which is used as the basis for `Reusable`.
pub struct ReusableAlloc {
    ptr: NonNull<u8>,
    layout: Layout,
}

unsafe impl Send for ReusableAlloc {}
unsafe impl Sync for ReusableAlloc {}

impl ReusableAlloc {
    /// Create a new empty `ReusableAlloc`.
    pub fn empty() -> ReusableAlloc {
        ReusableAlloc {
            ptr: NonNull::dangling(),
            layout: Layout::from_size_align(0, 1).unwrap(),
        }
    }

    /// Create a new `ReusableAlloc` from an existing allocation.
    pub unsafe fn from_raw(ptr: NonNull<u8>, layout: Layout) -> ReusableAlloc {
        ReusableAlloc {
            ptr,
            layout,
        }
    }

    /// Create a new `ReusableAlloc` which will fit a given layout.
    pub fn with_layout(layout: Layout) -> ReusableAlloc {
        let ptr = unsafe { NonNull::new(alloc(layout)).unwrap() };

        ReusableAlloc {
            ptr,
            layout,
        }
    }

    /// Get the layout of this allocation.
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Get a pointer to this allocation.
    pub fn as_ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    /// Resize this allocation to fit a given layout.
    pub fn resize_for_layout(mut self, layout: Layout) -> ReusableAlloc {
        if layout.align() <= self.layout.align() {
            // Alignment is okay, we can resize at worst.
            if layout.size() > self.layout.size() {
                unsafe {
                    self.ptr = NonNull::new(realloc(self.ptr.as_ptr(), self.layout, layout.size())).unwrap();
                    self.layout = layout;
                }
            }

            self
        } else {
            // We can't use the allocation at all, just allocate a new one.

            // Drop self first to avoid fragmentation a little.
            drop(self);
            ReusableAlloc::with_layout(layout)
        }
    }
}

impl Drop for ReusableAlloc {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            unsafe {
                dealloc(self.ptr.as_ptr(), self.layout);
            }
        }
    }
}

/// A `Reusable` `Box`.
pub struct Reusable<T: ?Sized> {
    ptr: NonNull<T>,
    layout: Layout,
    _phantom: PhantomData<*mut T>,
}

unsafe impl<T: ?Sized> Send for Reusable<T> {}
unsafe impl<T: ?Sized> Sync for Reusable<T> {}
impl<T: ?Sized> Unpin for Reusable<T> {}

impl<T> Reusable<T> {
    /// Allocate a new reusable.
    pub fn new(value: T) -> Reusable<T> {
        let alloc = ReusableAlloc::with_layout(Layout::new::<T>());
        Reusable::from_alloc(alloc, value)
    }

    /// Create a new `Reusable` from an existing allocation.
    pub fn from_alloc(alloc: ReusableAlloc, value: T) -> Reusable<T> {
        let layout = Layout::new::<T>();
        let alloc = alloc.resize_for_layout(layout);
        let layout = alloc.layout();
        let ptr = alloc.as_ptr().cast::<T>();
        forget(alloc);

        unsafe {
            std::ptr::write(ptr.as_ptr(), value);
        }

        Reusable {
            ptr,
            layout,
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized> Reusable<T> {
    /// Create a `Reusable` from a raw pointer and current layout.
    pub unsafe fn from_raw(ptr: NonNull<T>, layout: Layout) -> Reusable<T> {
        Reusable {
            ptr,
            layout,
            _phantom: PhantomData
        }
    }

    fn unsafe_forget(&self) -> ReusableAlloc {
        let ptr = self.ptr.cast::<u8>();

        unsafe {
            ReusableAlloc::from_raw(ptr, self.layout)
        }
    }

    fn unsafe_free(&self) -> ReusableAlloc {
        let ptr = self.ptr.cast::<u8>();

        unsafe {
            // Drop old value.
            std::ptr::read(ptr.as_ptr());
        }

        self.unsafe_forget()
    }

    /// Free this `Reusable`, returning the allocation.
    pub fn free(self) -> ReusableAlloc {
        let alloc = self.unsafe_free();
        forget(self);
        alloc
    }

    /// Forget this `Reusable`, returning the allocation.
    pub fn forget(self) -> ReusableAlloc {
        let alloc = self.unsafe_forget();
        forget(self);
        alloc
    }

    /// Get a pointer to the underlying allocation.
    pub fn as_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    /// Get the layout of the underlying allocation.
    pub fn layout(&self) -> Layout {
        self.layout
    }
}

impl<T: ?Sized> Drop for Reusable<T> {
    fn drop(&mut self) {
        self.unsafe_free();
    }
}

impl<T: ?Sized> Deref for Reusable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.as_ptr() }
    }
}

impl<T: ?Sized> DerefMut for Reusable<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.as_ptr() }
    }
}

impl<T: ?Sized + Future> Future for Reusable<T> {
    type Output = <T as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let f: Pin<&mut T> = unsafe { Pin::new_unchecked(&mut *self) };
        f.poll(cx)
    }
}
