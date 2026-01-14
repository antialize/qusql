//! Adds a drop handle wrapper, that calls a function with the value when it is dropped
use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

/// Container for Content, that calls Drop with content when dropped
pub(crate) struct HandleDrop<Content, Drop: FnOnce(Content)>(ManuallyDrop<(Content, Drop)>);

impl<Content, Drop: FnOnce(Content)> HandleDrop<Content, Drop> {
    /// Create a new instant
    pub(crate) fn new(content: Content, drop: Drop) -> Self {
        Self(ManuallyDrop::new((content, drop)))
    }

    /// Return content without calling drop
    pub(crate) fn release(mut self) -> Content {
        // Safety: This is safe because we call std::mem::forget immediately after, and
        // drop is the only other taker
        let (content, _) = unsafe { ManuallyDrop::take(&mut self.0) };
        std::mem::forget(self);
        content
    }
}

impl<Content, Drop: FnOnce(Content)> Deref for HandleDrop<Content, Drop> {
    type Target = Content;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}

impl<Content, Drop: FnOnce(Content)> DerefMut for HandleDrop<Content, Drop> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.0
    }
}

impl<Content, Drop: FnOnce(Content)> std::ops::Drop for HandleDrop<Content, Drop> {
    fn drop(&mut self) {
        // Safety: This is safe because all other takes call std::mem::forget
        let (content, drop) = unsafe { ManuallyDrop::take(&mut self.0) };
        drop(content)
    }
}
