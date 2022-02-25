use core::ops::Deref;

/// Represent a reference to T or a direct T
#[derive(Debug, Clone)]
pub enum RefOrVal<'a, T> {
    Ref(&'a T),
    Val(T),
}

impl<'a, T: PartialEq> PartialEq for RefOrVal<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl<'a, T: Eq> Eq for RefOrVal<'a, T> {}

impl<'a, T> Deref for RefOrVal<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            RefOrVal::Ref(v) => *v,
            RefOrVal::Val(v) => v,
        }
    }
}

impl<'a, T> RefOrVal<'a, T> {
    pub fn ref_clone(&'a self) -> Self {
        match self {
            RefOrVal::Ref(v) => RefOrVal::Ref(*v),
            RefOrVal::Val(v) => RefOrVal::Ref(v),
        }
    }
}
