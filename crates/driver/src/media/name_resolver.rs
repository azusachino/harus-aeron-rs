//! Name-resolution state shared by media transport setup and re-resolution.

use std::collections::BTreeMap;

use crate::error::Result;

use super::socket_address_parser::NameResolver;

/// Resolution-entry type identifiers used by Aeron's name-resolution protocol.
pub const RES_TYPE_NAME_TO_IP4_MD: u8 = 0;
pub const RES_TYPE_NAME_TO_IP6_MD: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub name: Vec<u8>,
    pub resolution_type: u8,
    pub port: u16,
    pub address: Vec<u8>,
    pub time_of_last_activity_ms: i64,
    pub deadline_ms: i64,
}

/// A bounded-by-expiration cache of name-to-address resolutions.
pub struct DriverNameResolverCache {
    timeout_ms: i64,
    entries: BTreeMap<(Vec<u8>, u8), CacheEntry>,
}

impl DriverNameResolverCache {
    pub fn new(timeout_ms: i64) -> Self {
        Self {
            timeout_ms,
            entries: BTreeMap::new(),
        }
    }

    pub fn add_or_update_entry(
        &mut self,
        name: &[u8],
        now_ms: i64,
        resolution_type: u8,
        address: &[u8],
        port: u16,
    ) -> CacheEntry {
        let entry = CacheEntry {
            name: name.to_vec(),
            resolution_type,
            port,
            address: address.to_vec(),
            time_of_last_activity_ms: now_ms,
            deadline_ms: now_ms.saturating_add(self.timeout_ms),
        };
        self.entries
            .insert((name.to_vec(), resolution_type), entry.clone());
        entry
    }

    pub fn lookup(&mut self, name: &[u8], resolution_type: u8, now_ms: i64) -> Option<CacheEntry> {
        let key = (name.to_vec(), resolution_type);
        let expired = self
            .entries
            .get(&key)
            .is_some_and(|entry| now_ms >= entry.deadline_ms);
        if expired {
            self.entries.remove(&key);
            return None;
        }
        self.entries.get(&key).cloned()
    }

    pub fn reset_iterator(&self) -> impl Iterator<Item = &CacheEntry> {
        self.entries.values()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Clock abstraction used to keep resolver timing independent from wall-clock code.
pub trait NanoClock {
    fn nano_time(&self) -> i64;
}

/// Receives the start and end of a resolver operation.
pub trait DutyCycleTracker {
    fn update(&self, now_ns: i64);
    fn measure_and_update(&self, now_ns: i64);
}

/// Decorates a resolver and accounts for the time spent in lookup and resolve calls, including
/// calls that return an error.
pub struct TimeTrackingNameResolver<R, C, T> {
    delegate: R,
    clock: C,
    tracker: T,
}

impl<R, C, T> TimeTrackingNameResolver<R, C, T>
where
    C: NanoClock,
    T: DutyCycleTracker,
{
    pub fn new(delegate: R, clock: C, tracker: T) -> Self {
        Self {
            delegate,
            clock,
            tracker,
        }
    }

    pub fn into_parts(self) -> (R, C, T) {
        (self.delegate, self.clock, self.tracker)
    }

    fn measure<F, V>(&self, operation: F) -> V
    where
        F: FnOnce(&R) -> V,
    {
        let begin_ns = self.clock.nano_time();
        self.tracker.update(begin_ns);
        let result = operation(&self.delegate);
        let end_ns = self.clock.nano_time();
        self.tracker.measure_and_update(end_ns);
        result
    }
}

impl<R, C, T> NameResolver for TimeTrackingNameResolver<R, C, T>
where
    R: NameResolver,
    C: NanoClock,
    T: DutyCycleTracker,
{
    fn resolve(
        &self,
        name: &str,
        uri_param_name: &str,
        is_re_resolution: bool,
    ) -> Result<std::net::IpAddr> {
        self.measure(|delegate| delegate.resolve(name, uri_param_name, is_re_resolution))
    }

    fn lookup(&self, name: &str, uri_param_name: &str, is_re_lookup: bool) -> String {
        self.measure(|delegate| delegate.lookup(name, uri_param_name, is_re_lookup))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::net::{IpAddr, Ipv4Addr};
    use std::rc::Rc;

    use super::*;
    use crate::media::socket_address_parser::DefaultNameResolver;

    #[test]
    fn cache_updates_and_iterates_entries_in_key_order() {
        let mut cache = DriverNameResolverCache::new(1000);
        cache.add_or_update_entry(b"A", 1, RES_TYPE_NAME_TO_IP4_MD, &[0, 0, 0, 0], 8050);
        cache.add_or_update_entry(b"AA", 2, RES_TYPE_NAME_TO_IP4_MD, &[1, 1, 1, 1], 8050);
        cache.add_or_update_entry(b"B", 3, RES_TYPE_NAME_TO_IP4_MD, &[2, 2, 2, 2], 8052);
        cache.add_or_update_entry(b"A", 4, RES_TYPE_NAME_TO_IP4_MD, &[127; 4], 9090);

        let entries: Vec<_> = cache.reset_iterator().collect();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_slice())
                .collect::<Vec<_>>(),
            vec![b"A".as_slice(), b"AA".as_slice(), b"B".as_slice()]
        );
        assert_eq!(entries[0].address, vec![127; 4]);
        assert_eq!(entries[0].deadline_ms, 1004);
    }

    #[test]
    fn cache_expires_entries_at_deadline() {
        let mut cache = DriverNameResolverCache::new(1000);
        cache.add_or_update_entry(b"A", 1, RES_TYPE_NAME_TO_IP4_MD, &[127, 0, 0, 1], 8050);
        assert!(cache.lookup(b"A", RES_TYPE_NAME_TO_IP4_MD, 1001).is_none());
        assert!(cache.is_empty());
    }

    struct TestClock {
        values: RefCell<Vec<i64>>,
    }

    impl NanoClock for TestClock {
        fn nano_time(&self) -> i64 {
            self.values.borrow_mut().remove(0)
        }
    }

    #[derive(Clone)]
    struct TestTracker {
        events: Rc<RefCell<Vec<i64>>>,
    }

    impl DutyCycleTracker for TestTracker {
        fn update(&self, now_ns: i64) {
            self.events.borrow_mut().push(now_ns);
        }

        fn measure_and_update(&self, now_ns: i64) {
            self.events.borrow_mut().push(now_ns);
        }
    }

    #[test]
    fn time_tracking_measures_lookup_and_resolve() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let resolver = TimeTrackingNameResolver::new(
            DefaultNameResolver,
            TestClock {
                values: RefCell::new(vec![10, 20, 30, 40]),
            },
            TestTracker {
                events: events.clone(),
            },
        );
        assert_eq!(
            resolver.lookup("localhost:55", "endpoint", false),
            "localhost:55"
        );
        assert_eq!(
            resolver.resolve("127.0.0.1", "endpoint", false).unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        assert_eq!(*events.borrow(), vec![10, 20, 30, 40]);
    }
}
