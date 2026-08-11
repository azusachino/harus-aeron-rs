//! Durable metadata for terms and snapshots in a cluster recording log.
//!
//! This is the portable metadata core of Aeron's `RecordingLog`. Archive
//! recording, replay, and multi-node recovery remain separate boundaries.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub const RECORDING_LOG_FILE_NAME: &str = "recording.log";
pub const NULL_VALUE: i64 = -1;
pub const ENTRY_TYPE_TERM: u8 = 0;
pub const ENTRY_TYPE_SNAPSHOT: u8 = 1;
pub const ENTRY_TYPE_STANDBY_SNAPSHOT: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingLogEntry {
    pub recording_id: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub log_position: i64,
    pub timestamp: i64,
    pub service_id: i32,
    pub entry_type: u8,
    pub archive_endpoint: Option<String>,
    pub is_valid: bool,
    pub entry_index: usize,
}

impl RecordingLogEntry {
    pub fn is_snapshot(&self) -> bool {
        self.entry_type == ENTRY_TYPE_SNAPSHOT
    }

    pub fn is_standby_snapshot(&self) -> bool {
        self.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT
    }
}

pub struct RecordingLog {
    path: PathBuf,
    entries: Vec<RecordingLogEntry>,
}

impl RecordingLog {
    pub fn open(cluster_dir: impl AsRef<Path>) -> io::Result<Self> {
        fs::create_dir_all(cluster_dir.as_ref())?;
        let path = cluster_dir.as_ref().join(RECORDING_LOG_FILE_NAME);
        let mut log = Self {
            path,
            entries: Vec::new(),
        };
        if log.path.exists() {
            log.load()?;
        } else {
            log.persist()?;
        }
        Ok(log)
    }

    pub fn entries(&self) -> &[RecordingLogEntry] {
        &self.entries
    }

    pub fn next_entry_index(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| entry.entry_index)
            .max()
            .map_or(0, |index| index + 1)
    }

    pub fn append_term(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        timestamp: i64,
    ) -> io::Result<usize> {
        self.append(RecordingLogEntry {
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position: NULL_VALUE,
            timestamp,
            service_id: NULL_VALUE as i32,
            entry_type: ENTRY_TYPE_TERM,
            archive_endpoint: None,
            is_valid: true,
            entry_index: self.next_entry_index(),
        })
    }

    pub fn append_snapshot(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        service_id: i32,
    ) -> io::Result<usize> {
        self.append(RecordingLogEntry {
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            entry_type: ENTRY_TYPE_SNAPSHOT,
            archive_endpoint: None,
            is_valid: true,
            entry_index: self.next_entry_index(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_standby_snapshot(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        service_id: i32,
        archive_endpoint: impl Into<String>,
    ) -> io::Result<usize> {
        let archive_endpoint = archive_endpoint.into();
        if archive_endpoint.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "standby snapshots require an archive endpoint",
            ));
        }
        self.append(RecordingLogEntry {
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            entry_type: ENTRY_TYPE_STANDBY_SNAPSHOT,
            archive_endpoint: Some(archive_endpoint),
            is_valid: true,
            entry_index: self.next_entry_index(),
        })
    }

    pub fn commit_log_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
    ) -> io::Result<bool> {
        let Some(entry) = self.entries.iter_mut().find(|entry| {
            entry.is_valid
                && entry.entry_type == ENTRY_TYPE_TERM
                && entry.leadership_term_id == leadership_term_id
        }) else {
            return Ok(false);
        };
        entry.log_position = log_position;
        self.persist()?;
        Ok(true)
    }

    pub fn invalidate_entry(&mut self, entry_index: usize) -> io::Result<bool> {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.entry_index == entry_index)
        else {
            return Ok(false);
        };
        entry.is_valid = false;
        self.persist()?;
        Ok(true)
    }

    pub fn remove_entry(
        &mut self,
        leadership_term_id: i64,
        entry_index: usize,
    ) -> io::Result<bool> {
        let length = self.entries.len();
        self.entries.retain(|entry| {
            !(entry.leadership_term_id == leadership_term_id && entry.entry_index == entry_index)
        });
        if self.entries.len() == length {
            return Ok(false);
        }
        self.persist()?;
        Ok(true)
    }

    pub fn get_latest_snapshot(&self, service_id: i32) -> Option<&RecordingLogEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_valid && entry.is_snapshot() && entry.service_id == service_id)
            .max_by_key(|entry| entry.entry_index)
    }

    pub fn find_last_term(&self) -> Option<&RecordingLogEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_valid && entry.entry_type == ENTRY_TYPE_TERM)
            .max_by_key(|entry| entry.leadership_term_id)
    }

    pub fn find_term(&self, leadership_term_id: i64) -> Option<&RecordingLogEntry> {
        self.entries.iter().find(|entry| {
            entry.is_valid
                && entry.entry_type == ENTRY_TYPE_TERM
                && entry.leadership_term_id == leadership_term_id
        })
    }

    pub fn is_unknown(&self, leadership_term_id: i64) -> bool {
        self.find_term(leadership_term_id).is_none()
    }

    pub fn get_term_timestamp(&self, leadership_term_id: i64) -> i64 {
        self.find_term(leadership_term_id)
            .map_or(NULL_VALUE, |entry| entry.timestamp)
    }

    pub fn invalidate_latest_snapshot(&mut self) -> io::Result<bool> {
        let Some(latest) = self
            .entries
            .iter()
            .filter(|entry| entry.is_valid && entry.is_snapshot())
            .max_by_key(|entry| entry.entry_index)
            .cloned()
        else {
            return Ok(false);
        };
        for entry in &mut self.entries {
            if entry.is_valid
                && entry.is_snapshot()
                && entry.leadership_term_id == latest.leadership_term_id
                && entry.log_position == latest.log_position
                && entry.timestamp == latest.timestamp
            {
                entry.is_valid = false;
            }
        }
        self.persist()?;
        Ok(true)
    }

    pub fn truncate_after(&mut self, log_position: i64) -> io::Result<usize> {
        let before = self.entries.len();
        self.entries.retain(|entry| {
            entry.entry_type == ENTRY_TYPE_TERM
                || entry.log_position == NULL_VALUE
                || entry.log_position <= log_position
        });
        let removed = before - self.entries.len();
        if removed > 0 {
            self.persist()?;
        }
        Ok(removed)
    }

    pub fn snapshot_order<'a>(
        entries: impl IntoIterator<Item = &'a RecordingLogEntry>,
    ) -> Vec<&'a RecordingLogEntry> {
        let mut snapshots = entries
            .into_iter()
            .filter(|entry| entry.is_valid && entry.is_snapshot())
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|entry| {
            (
                entry.leadership_term_id,
                entry.log_position,
                entry.service_id,
            )
        });
        snapshots
    }

    fn append(&mut self, entry: RecordingLogEntry) -> io::Result<usize> {
        if entry.recording_id == NULL_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recording id cannot be null",
            ));
        }
        let index = entry.entry_index;
        self.entries.push(entry);
        self.persist()?;
        Ok(index)
    }

    fn persist(&self) -> io::Result<()> {
        let mut file = File::create(&self.path)?;
        writeln!(file, "AERON-RECORDING-LOG-1")?;
        for entry in &self.entries {
            writeln!(
                file,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                entry.recording_id,
                entry.leadership_term_id,
                entry.term_base_log_position,
                entry.log_position,
                entry.timestamp,
                entry.service_id,
                entry.entry_type,
                entry.archive_endpoint.as_deref().unwrap_or(""),
                u8::from(entry.is_valid),
                entry.entry_index
            )?;
        }
        file.sync_data()
    }

    fn load(&mut self) -> io::Result<()> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut lines = BufReader::new(file).lines();
        match lines.next().transpose()? {
            Some(header) if header == "AERON-RECORDING-LOG-1" => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid recording log header",
                ));
            }
        }
        self.entries.clear();
        for line in lines {
            let line = line?;
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields.len() != 10 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid recording log entry",
                ));
            }
            self.entries.push(RecordingLogEntry {
                recording_id: parse(fields[0])?,
                leadership_term_id: parse(fields[1])?,
                term_base_log_position: parse(fields[2])?,
                log_position: parse(fields[3])?,
                timestamp: parse(fields[4])?,
                service_id: parse(fields[5])?,
                entry_type: parse(fields[6])?,
                archive_endpoint: (!fields[7].is_empty()).then(|| fields[7].to_string()),
                is_valid: parse::<u8>(fields[8])? != 0,
                entry_index: parse(fields[9])?,
            });
        }
        Ok(())
    }
}

fn parse<T: std::str::FromStr>(value: &str) -> io::Result<T> {
    value
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid recording log field"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "aeron-cluster-recording-log-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn should_append_and_reload_latest_snapshot() {
        let dir = temp_dir();
        {
            let mut log = RecordingLog::open(&dir).unwrap();
            log.append_snapshot(1, 3, 2, 777, 4, 0).unwrap();
        }
        let log = RecordingLog::open(&dir).unwrap();
        let snapshot = log.get_latest_snapshot(0).unwrap();
        assert_eq!(snapshot.recording_id, 1);
        assert_eq!(snapshot.log_position, 777);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn should_commit_terms_and_invalidate_snapshot_groups() {
        let dir = temp_dir();
        let mut log = RecordingLog::open(&dir).unwrap();
        log.append_term(1, 3, 2, 4).unwrap();
        log.commit_log_position(3, 777).unwrap();
        log.append_snapshot(2, 3, 2, 777, 5, 0).unwrap();
        log.append_snapshot(3, 3, 2, 777, 5, 1).unwrap();
        assert_eq!(log.find_last_term().unwrap().log_position, 777);
        assert!(log.invalidate_latest_snapshot().unwrap());
        assert!(log.get_latest_snapshot(0).is_none());
        assert!(log.get_latest_snapshot(1).is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn should_truncate_snapshot_metadata_and_preserve_terms() {
        let dir = temp_dir();
        let mut log = RecordingLog::open(&dir).unwrap();
        log.append_term(1, 1, 0, 0).unwrap();
        log.append_snapshot(2, 1, 0, 100, 1, 0).unwrap();
        log.append_snapshot(3, 1, 0, 200, 2, 0).unwrap();
        assert_eq!(log.truncate_after(100).unwrap(), 1);
        assert_eq!(log.entries().len(), 2);
        assert!(log.find_term(1).is_some());
        let _ = fs::remove_dir_all(dir);
    }
}
