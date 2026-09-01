/// In-memory backend to be used for testing
pub mod in_memory_backend {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::RwLock,
    };

    use bytes::{Bytes, BytesMut};
    use enum_map::EnumMap;

    use rustic_core::{
        BytesList, ErrorKind, FileType, Id, ReadBackend, RusticError, RusticResult, WarmupStatus,
        WriteBackend,
    };

    #[derive(Debug)]
    /// In-Memory backend to be used for testing
    pub struct InMemoryBackend {
        map: RwLock<EnumMap<FileType, BTreeMap<Id, Bytes>>>,
        is_cold: bool,
        warm: RwLock<EnumMap<FileType, BTreeSet<Id>>>,
        archive_class: Option<String>,
        /// After `warm_up`, this many `warmup_status` calls return `Warming` before `Warm`.
        polls_until_warm: u32,
        remaining_polls: RwLock<EnumMap<FileType, BTreeMap<Id, u32>>>,
    }

    impl Clone for InMemoryBackend {
        fn clone(&self) -> Self {
            let inner_map = self.map.read().unwrap();
            let inner_warm = self.warm.read().unwrap();
            let inner_polls = self.remaining_polls.read().unwrap();
            Self {
                map: RwLock::new(EnumMap::from_fn(|tpe| inner_map[tpe].clone())),
                is_cold: self.is_cold,
                warm: RwLock::new(EnumMap::from_fn(|tpe| inner_warm[tpe].clone())),
                archive_class: self.archive_class.clone(),
                polls_until_warm: self.polls_until_warm,
                remaining_polls: RwLock::new(EnumMap::from_fn(|tpe| inner_polls[tpe].clone())),
            }
        }
    }

    impl InMemoryBackend {
        fn empty(is_cold: bool) -> Self {
            Self {
                map: RwLock::new(EnumMap::from_fn(|_| BTreeMap::new())),
                is_cold,
                warm: RwLock::new(EnumMap::from_fn(|_| BTreeSet::new())),
                archive_class: None,
                polls_until_warm: 0,
                remaining_polls: RwLock::new(EnumMap::from_fn(|_| BTreeMap::new())),
            }
        }

        /// Create a new (empty) `InMemoryBackend`
        #[must_use]
        pub fn new() -> Self {
            Self::empty(false)
        }

        /// Create a new (empty) cold `InMemoryBackend`
        #[must_use]
        pub fn new_cold() -> Self {
            Self::empty(true)
        }

        /// Cold backend whose `warmup_status` stays `Warming` for `polls` status checks after `warm_up`.
        #[must_use]
        pub fn new_cold_with_polls(polls: u32) -> Self {
            let mut be = Self::empty(true);
            be.polls_until_warm = polls;
            be
        }

        /// Set a fake archive class so prune treats this backend like Glacier.
        #[must_use]
        pub fn with_archive_class(mut self, class: impl Into<String>) -> Self {
            self.archive_class = Some(class.into());
            self
        }
    }

    impl Default for InMemoryBackend {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ReadBackend for InMemoryBackend {
        fn location(&self) -> String {
            "test".to_string()
        }

        fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
            Ok(self.map.read().unwrap()[tpe]
                .iter()
                .map(|(id, byte)| {
                    (
                        *id,
                        u32::try_from(byte.len()).expect("byte length is too large"),
                    )
                })
                .collect())
        }

        fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
            if self.is_cold && !self.warm.read().unwrap()[tpe].contains(id) {
                return Err(RusticError::new(
                    ErrorKind::Backend,
                    "tpe {tpe} id `{id}` is not warmed-up",
                )
                .attach_context("tpe", tpe.to_string())
                .attach_context("id", id.to_string()));
            }
            Ok(self.map.read().unwrap()[tpe]
                .get(id)
                .ok_or_else(|| {
                    RusticError::new(
                        ErrorKind::Backend,
                        "Element tpe: {tpe}, id: {id} does not exist in backend",
                    )
                    .attach_context("tpe", tpe.to_string())
                    .attach_context("id", id.to_string())
                })?
                .clone())
        }

        fn read_partial(
            &self,
            tpe: FileType,
            id: &Id,
            _cacheable: bool,
            offset: u32,
            length: u32,
        ) -> RusticResult<Bytes> {
            if self.is_cold && !self.warm.read().unwrap()[tpe].contains(id) {
                return Err(RusticError::new(
                    ErrorKind::Backend,
                    "tpe {tpe} id `{id}` is not warmed-up",
                )
                .attach_context("tpe", tpe.to_string())
                .attach_context("id", id.to_string()));
            }
            Ok(
                self.map.read().unwrap()[tpe][id]
                    .slice(offset as usize..(offset + length) as usize),
            )
        }

        fn warmup_path(&self, tpe: FileType, id: &Id) -> String {
            // For in-memory backend, return a simple identifier
            // Since this is a testing backend, we can return a formatted path
            let hex_id = id.to_hex();
            match tpe {
                FileType::Config => "config".to_string(),
                FileType::Pack => format!("data/{}/{}", &hex_id[0..2], hex_id.as_str()),
                _ => format!("{}/{}", tpe.dirname(), hex_id.as_str()),
            }
        }

        fn needs_warm_up(&self) -> bool {
            self.is_cold
        }

        fn warm_up(&self, tpe: FileType, id: &Id) -> RusticResult<()> {
            if self.is_cold {
                if self.polls_until_warm == 0 {
                    _ = self.warm.write().unwrap()[tpe].insert(*id);
                } else {
                    _ = self.remaining_polls.write().unwrap()[tpe]
                        .insert(*id, self.polls_until_warm);
                }
            }
            Ok(())
        }

        fn reports_warmup_status(&self) -> bool {
            self.is_cold
        }

        fn warmup_status(&self, tpe: FileType, id: &Id) -> RusticResult<WarmupStatus> {
            if !self.is_cold || self.warm.read().unwrap()[tpe].contains(id) {
                return Ok(WarmupStatus::Warm);
            }
            let mut remaining = self.remaining_polls.write().unwrap();
            if let Some(left) = remaining[tpe].get_mut(id) {
                if *left == 0 {
                    _ = self.warm.write().unwrap()[tpe].insert(*id);
                    return Ok(WarmupStatus::Warm);
                }
                *left -= 1;
                if *left == 0 {
                    _ = remaining[tpe].remove(id);
                    _ = self.warm.write().unwrap()[tpe].insert(*id);
                    return Ok(WarmupStatus::Warm);
                }
                return Ok(WarmupStatus::Warming);
            }
            Ok(WarmupStatus::Cold)
        }

        fn archive_class(&self) -> Option<&str> {
            self.archive_class.as_deref()
        }
    }

    impl WriteBackend for InMemoryBackend {
        fn create(&self) -> RusticResult<()> {
            Ok(())
        }

        fn write_bytes(
            &self,
            tpe: FileType,
            id: &Id,
            _cacheable: bool,
            content: BytesList,
        ) -> RusticResult<()> {
            let mut bytes = BytesMut::new();
            for input in content.slice() {
                bytes.extend_from_slice(input);
            }
            let bytes = bytes.freeze();
            if self.map.write().unwrap()[tpe].insert(*id, bytes).is_some() {
                return Err(
                    RusticError::new(ErrorKind::Backend, "ID `{id}` already exists.")
                        .attach_context("id", id.to_string()),
                );
            }

            Ok(())
        }

        fn remove(&self, tpe: FileType, id: &Id, _cacheable: bool) -> RusticResult<()> {
            if self.map.write().unwrap()[tpe].remove(id).is_none() {
                return Err(
                    RusticError::new(ErrorKind::Backend, "ID `{id}` does not exist.")
                        .attach_context("id", id.to_string()),
                );
            }
            Ok(())
        }
    }
}
