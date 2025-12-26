//! LRU cache with Cow<'static, str> keys
use std::{borrow::Cow, collections::HashMap, hash::Hash, ptr::NonNull};

/// A key borrowed from the hash_map value
struct Key(NonNull<u8>, usize);

/// Safety: It is safe to send a key across a thread boundary
unsafe impl Send for Key {}

impl Key {
    /// Construct a new key from a COV string. This object must not live longer that the backing
    /// Cow<str> though it may be moved
    unsafe fn new(key: &str) -> Key {
        // Safety: A str cannot be null
        let v = unsafe { NonNull::new_unchecked(key.as_ptr() as *mut _) };
        Key(v, key.len())
    }

    /// Return the content of the string
    fn content(&self) -> &'_ str {
        // Safety: This is safe since we assume that Cow<str> object is still around
        let slice = unsafe { std::slice::from_raw_parts(self.0.as_ptr(), self.1) };
        // Safety: The slice was constructed from a str
        unsafe { str::from_utf8_unchecked(slice) }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.content() == other.content()
    }
}

impl Eq for Key {}

impl Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.content().hash(state);
    }
}

/// The value boxed in the hash map
struct Value<V> {
    /// The actual keys
    key: Cow<'static, str>,
    /// The user supplied values
    value: V,
    /// The prev pointer in the LRU list
    prev: *mut Value<V>,
    /// The next pointer in the LRU list
    next: *mut Value<V>,
}

/// Safety: It is safe to send a value across a thread boundary
unsafe impl<V: Send> Send for Value<V> {}

/// The lru double linked list
struct LRUList<V> {
    /// The first entry in the list, or null if it is empty
    first: *mut Value<V>,
    /// The last entry in the list, or null if it is empty
    last: *mut Value<V>,
    /// The user supplied maximum size of the list
    max_size: usize,
}

/// Safety: It is safe to list a value across a thread boundary
unsafe impl<V: Send> Send for LRUList<V> {}

/// LRU cached with Cow<'str, keys>
pub(crate) struct LRUCache<V> {
    /// The map of values
    map: HashMap<Key, Box<Value<V>>>,
    /// Linked list of values in use order, most recently accessed element first.
    list: LRUList<V>,
}

/// Occupied entry in the [LRUCache]
pub struct OccupiedEntry<'a, V> {
    /// The linked list
    list: &'a mut LRUList<V>,
    /// The underlying hashmap entry
    entry: std::collections::hash_map::OccupiedEntry<'a, Key, Box<Value<V>>>,
}

impl<'a, V> OccupiedEntry<'a, V> {
    /// Bump value to the top of the lru
    pub fn bump(&mut self) {
        let value = self.entry.get_mut();
        let prev = value.prev;
        let next = value.next;
        if !prev.is_null() {
            let e = value.as_mut();

            // Safety: We know that prev is not null
            unsafe { &mut *prev }.next = next;
            if !next.is_null() {
                // Safety: We know that next is not null
                unsafe { &mut *next }.prev = prev;
            } else {
                self.list.last = prev;
            }
            // Insert entry in the beginning of the list
            e.prev = std::ptr::null_mut();
            e.next = self.list.first;
            // Safety: The list is none empty  so we have a first element
            unsafe { &mut *self.list.first }.prev = e;
            self.list.first = e;
        }
    }

    /// Insert new value into slot returning the old value
    pub fn insert(&mut self, value: V) -> V {
        std::mem::replace(&mut self.entry.get_mut().value, value)
    }

    /// Get a reference to the value
    pub fn get(&self) -> &V {
        &self.entry.get().value
    }

    /// Get a reference to the key
    pub fn key(&self) -> &str {
        self.entry.key().content()
    }

    /// Convert entry into a mutable reference to the value
    pub fn into_mut(self) -> &'a mut V {
        &mut self.entry.into_mut().value
    }
}

/// Vacant entry in the [LRUCache]
pub struct VacantEntry<'a, V> {
    /// The linked list
    list: &'a mut LRUList<V>,
    /// The underlying hashmap entry
    entry: std::collections::hash_map::VacantEntry<'a, Key, Box<Value<V>>>,
    /// Pointer to the hash map. It need to be a pointer since entry is a mut ref to the same map
    map: *mut HashMap<Key, Box<Value<V>>>,
    /// The key we might be about to insert
    key: Cow<'static, str>,
}

/// Safety: It is safe to send across a thread boundary
unsafe impl<'a, V> Send for VacantEntry<'a, V> {}

impl<'a, V> VacantEntry<'a, V> {
    /// Insert element into vacant slot, possible
    /// returning old item to dispose of
    pub fn insert(self, value: V) -> (&'a mut V, Option<(Cow<'static, str>, V)>) {
        let mut value = Box::new(Value {
            key: self.key,
            value,
            next: self.list.first,
            prev: std::ptr::null_mut(),
        });
        if self.list.first.is_null() {
            self.list.first = value.as_mut();
            self.list.last = value.as_mut();
            (&mut self.entry.insert(value).value, None)
        } else {
            // Safety: The list is none empty
            unsafe { &mut *self.list.first }.prev = value.as_mut();
            self.list.first = value.as_mut();
            let r = &mut self.entry.insert(value).value;
            // Safety: Mutating the hashmap will not invalidate the r reference a long as
            // we do not remove that entry
            let map = unsafe { &mut *self.map };
            if map.len() > self.list.max_size {
                // Safety: We know that the list is not empty and that the just inserted element is not the last
                let key = &unsafe { &mut *self.list.last }.key;
                // Safety: We do not keep the key around longer than the value
                let key = unsafe { Key::new(key) };
                let value = *map.remove(&key).expect("Logic error in lru");
                self.list.last = value.prev;
                // Safety: We know that there are at least 2 entries in the map
                unsafe { &mut *value.prev }.next = std::ptr::null_mut();
                (r, Some((value.key, value.value)))
            } else {
                (r, None)
            }
        }
    }

    /// Reference to the key we are about to insert
    pub fn key(&self) -> &str {
        self.entry.key().content()
    }
}

/// Entry in the [LRUCache]
pub enum Entry<'a, V> {
    /// Entry in the map with the supplied key
    Occupied(OccupiedEntry<'a, V>),
    /// Vacant space in the map where the entry with the supplied key can be inserted
    Vacant(VacantEntry<'a, V>),
}

impl<V> LRUCache<V> {
    /// Construct a new lru cache that may contain at most max_size entries.
    ///
    /// Panics if max_size < 2
    pub fn new(max_size: usize) -> Self {
        if max_size < 2 {
            panic!("Max size should be 3 or greater");
        }
        Self {
            map: HashMap::new(),
            list: LRUList {
                first: std::ptr::null_mut(),
                last: std::ptr::null_mut(),
                max_size,
            },
        }
    }

    /// Find a entry in the hashmap for the given key
    pub fn entry<'a>(&'a mut self, key: Cow<'static, str>) -> Entry<'a, V> {
        // Safety: The construction of the VacantEntry ensures that the key will stick
        // around after being moved into the [Value]
        let k = unsafe { Key::new(&key) };
        let map = &mut self.map as *mut _;
        match self.map.entry(k) {
            std::collections::hash_map::Entry::Occupied(entry) => Entry::Occupied(OccupiedEntry {
                list: &mut self.list,
                entry,
            }),
            std::collections::hash_map::Entry::Vacant(entry) => Entry::Vacant(VacantEntry {
                list: &mut self.list,
                entry,
                key,
                map,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::lru::{Entry, LRUCache};

    #[test]
    fn test_small() {
        let mut map = LRUCache::new(2);
        match map.entry("a".into()) {
            Entry::Occupied(_) => panic!("Not expected"),
            Entry::Vacant(e) => {
                e.insert(1);
            }
        }
        match map.entry("b".into()) {
            Entry::Occupied(_) => panic!("Not expected"),
            Entry::Vacant(e) => {
                e.insert(2);
            }
        }
        match map.entry("a".into()) {
            Entry::Occupied(mut e) => e.bump(),
            Entry::Vacant(_) => panic!("Not expected"),
        }
        // Insert c, should drop b
        match map.entry("c".into()) {
            Entry::Occupied(_) => panic!("Not expected"),
            Entry::Vacant(e) => {
                let (_, v) = e.insert(3);
                let (k, v) = v.expect("Should get b");
                assert_eq!(k, "b");
                assert_eq!(v, 2);
            }
        }
        match map.entry("d".into()) {
            Entry::Occupied(_) => panic!("Not expected"),
            Entry::Vacant(e) => {
                let (_, v) = e.insert(4);
                let (k, v) = v.expect("Should get a");
                assert_eq!(k, "a");
                assert_eq!(v, 1);
            }
        }
    }

    struct Rng(u64, u64);
    impl Rng {
        fn new() -> Self {
            Self(0xf4dbdf2183dcefb7, 0x1ad5be0d6dd28e9b)
        }
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            let y = self.1;
            self.0 = y;
            x ^= x << 32;
            self.1 = x ^ y ^ (x >> 17) ^ (y >> 26);
            self.1.wrapping_add(y)
        }
    }

    #[test]
    fn randomized() {
        let mut value_map: HashMap<u32, (u32, usize)> = HashMap::new();
        let mut c = LRUCache::<u32>::new(111);

        let mut rng = Rng::new();
        for time in 0..100000 {
            match rng.next() & 0xff {
                ..200 => {
                    // Check that a random value is there
                    if !value_map.is_empty() {
                        let n = (rng.next() % (value_map.len() as u64)) as usize;
                        let (k, (v, access_time)) = value_map.iter_mut().nth(n).unwrap();

                        *access_time = time;

                        let ks = format!("{}", k);

                        let f = match c.entry(ks.into()) {
                            Entry::Occupied(mut e) => {
                                e.bump();
                                *e.get()
                            }
                            Entry::Vacant(_) => {
                                panic!("Should be there")
                            }
                        };
                        assert_eq!(f, *v);
                    }
                }
                200.. => {
                    let k = (rng.next() & 0xFFFFFF) as u32;
                    if value_map.contains_key(&k) {
                        continue;
                    }
                    let v = (rng.next() & 0xFFFFFF) as u32;
                    value_map.insert(k, (v, time));
                    let ks = format!("{}", k);
                    match c.entry(ks.into()) {
                        Entry::Occupied(_) => panic!(),
                        Entry::Vacant(e) => {
                            e.insert(v);
                        }
                    }

                    while value_map.len() > 111 {
                        let (k, _) = value_map.iter().min_by_key(|(_, (_, t))| *t).unwrap();
                        let k = *k;
                        let ks = format!("{}", k);
                        value_map.remove(&k);
                        match c.entry(ks.into()) {
                            Entry::Occupied(_) => panic!(),
                            Entry::Vacant(_) => {}
                        }
                    }
                }
            }
        }
    }
}
