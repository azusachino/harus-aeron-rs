//! Durable node-local election metadata.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"AERONNS1";
const VERSION: u32 = 1;
const FILE_LENGTH: u64 = 8 + 4 + (8 * 3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateTerm {
    candidate_term_id: i64,
    log_position: i64,
    timestamp_ms: i64,
}

impl CandidateTerm {
    pub fn candidate_term_id(self) -> i64 {
        self.candidate_term_id
    }

    pub fn log_position(self) -> i64 {
        self.log_position
    }

    pub fn timestamp(self) -> i64 {
        self.timestamp_ms
    }
}

pub struct NodeStateFile {
    path: PathBuf,
    file: File,
    candidate_term: CandidateTerm,
}

impl NodeStateFile {
    pub const FILENAME: &'static str = "node-state.dat";

    pub fn open(
        cluster_dir: impl AsRef<Path>,
        create_new: bool,
        _sync_level: u32,
    ) -> io::Result<Self> {
        let path = cluster_dir.as_ref().join(Self::FILENAME);
        let file = if create_new {
            fs::create_dir_all(cluster_dir.as_ref())?;
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?
        } else {
            OpenOptions::new().read(true).write(true).open(&path)?
        };

        let mut state = Self {
            path,
            file,
            candidate_term: CandidateTerm {
                candidate_term_id: -1,
                log_position: -1,
                timestamp_ms: -1,
            },
        };

        if state.file.metadata()?.len() == 0 {
            state.persist()?;
        } else {
            state.load()?;
        }

        Ok(state)
    }

    pub fn candidate_term(&self) -> CandidateTerm {
        self.candidate_term
    }

    pub fn update_candidate_term_id(
        &mut self,
        candidate_term_id: i64,
        log_position: i64,
        timestamp_ms: i64,
    ) -> io::Result<()> {
        self.candidate_term = CandidateTerm {
            candidate_term_id,
            log_position,
            timestamp_ms,
        };
        self.persist()
    }

    pub fn propose_max_candidate_term_id(
        &mut self,
        candidate_term_id: i64,
        log_position: i64,
        timestamp_ms: i64,
    ) -> io::Result<i64> {
        if candidate_term_id > self.candidate_term.candidate_term_id {
            self.update_candidate_term_id(candidate_term_id, log_position, timestamp_ms)?;
        }
        Ok(self.candidate_term.candidate_term_id)
    }

    fn persist(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(MAGIC)?;
        self.file.write_all(&VERSION.to_le_bytes())?;
        self.file
            .write_all(&self.candidate_term.candidate_term_id.to_le_bytes())?;
        self.file
            .write_all(&self.candidate_term.log_position.to_le_bytes())?;
        self.file
            .write_all(&self.candidate_term.timestamp_ms.to_le_bytes())?;
        self.file.set_len(FILE_LENGTH)?;
        self.file.sync_data()
    }

    fn load(&mut self) -> io::Result<()> {
        let mut bytes = [0_u8; FILE_LENGTH as usize];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut bytes)?;
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid node state magic",
            ));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported node state version: {version}"),
            ));
        }
        self.candidate_term = CandidateTerm {
            candidate_term_id: i64::from_le_bytes(bytes[12..20].try_into().unwrap()),
            log_position: i64::from_le_bytes(bytes[20..28].try_into().unwrap()),
            timestamp_ms: i64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        };
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "aeron-cluster-node-state-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn should_require_existing_file_when_not_creating() {
        let dir = temp_dir();
        assert!(NodeStateFile::open(&dir, false, 1).is_err());
    }

    #[test]
    fn should_create_and_initialize_node_state() {
        let dir = temp_dir();
        let state = NodeStateFile::open(&dir, true, 1).unwrap();
        assert!(state.path().exists());
        assert_eq!(state.candidate_term().candidate_term_id(), -1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn should_persist_and_monotonically_propose_candidate_terms() {
        let dir = temp_dir();
        {
            let mut state = NodeStateFile::open(&dir, true, 1).unwrap();
            state.update_candidate_term_id(5, 10, 20).unwrap();
            assert_eq!(state.propose_max_candidate_term_id(4, 30, 40).unwrap(), 5);
            assert_eq!(state.propose_max_candidate_term_id(6, 50, 60).unwrap(), 6);
        }
        let state = NodeStateFile::open(&dir, false, 1).unwrap();
        assert_eq!(state.candidate_term().candidate_term_id(), 6);
        assert_eq!(state.candidate_term().log_position(), 50);
        assert_eq!(state.candidate_term().timestamp(), 60);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn should_reject_unknown_version() {
        let dir = temp_dir();
        let state = NodeStateFile::open(&dir, true, 1).unwrap();
        let mut file = OpenOptions::new().write(true).open(state.path()).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&2_u32.to_le_bytes()).unwrap();
        file.sync_data().unwrap();
        drop(file);
        assert!(NodeStateFile::open(&dir, false, 1).is_err());
        let _ = fs::remove_dir_all(dir);
    }
}
