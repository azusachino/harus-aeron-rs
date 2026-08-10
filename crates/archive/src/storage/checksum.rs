//! Checksum implementations and the Archive checksum factory.

use std::fmt;

pub trait Checksum: Send + Sync {
    fn compute(&self, data: &[u8]) -> u32;
}

#[derive(Debug, Default)]
pub struct Crc32;

#[derive(Debug, Default)]
pub struct Crc32c;

impl Checksum for Crc32 {
    fn compute(&self, data: &[u8]) -> u32 {
        crc(data, 0xedb8_8320)
    }
}

impl Checksum for Crc32c {
    fn compute(&self, data: &[u8]) -> u32 {
        crc(data, 0x82f6_3b78)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChecksumError {
    EmptyClassName,
    ClassNotFound(String),
}

impl fmt::Display for ChecksumError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyClassName => formatter.write_str("class name is empty"),
            Self::ClassNotFound(class_name) => {
                write!(formatter, "checksum class not found: {class_name}")
            }
        }
    }
}

impl std::error::Error for ChecksumError {}

pub struct Checksums;

static CRC_32: Crc32 = Crc32;
static CRC_32C: Crc32c = Crc32c;

impl Checksums {
    pub fn crc32() -> &'static dyn Checksum {
        &CRC_32
    }

    pub fn crc32c() -> &'static dyn Checksum {
        &CRC_32C
    }

    pub fn new_instance(class_name: Option<&str>) -> Result<&'static dyn Checksum, ChecksumError> {
        let class_name = class_name.filter(|name| !name.is_empty());
        let Some(class_name) = class_name else {
            return Err(ChecksumError::EmptyClassName);
        };

        match class_name {
            "CRC-32" | "io.aeron.archive.checksum.Crc32" | "org.agrona.checksum.Crc32" => {
                Ok(Self::crc32())
            }
            "CRC-32C" | "io.aeron.archive.checksum.Crc32c" | "org.agrona.checksum.Crc32c" => {
                Ok(Self::crc32c())
            }
            _ => Err(ChecksumError::ClassNotFound(class_name.to_owned())),
        }
    }

    /// Construct a custom checksum using Rust's type system instead of Java reflection.
    pub fn new_instance_of<T>() -> Box<dyn Checksum>
    where
        T: Checksum + Default + 'static,
    {
        Box::new(T::default())
    }
}

fn crc(data: &[u8], polynomial: u32) -> u32 {
    let mut value = u32::MAX;
    for byte in data {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = if value & 1 == 0 {
                value >> 1
            } else {
                (value >> 1) ^ polynomial
            };
        }
    }
    !value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestChecksum(u8);

    impl Checksum for TestChecksum {
        fn compute(&self, _data: &[u8]) -> u32 {
            u32::from(self.0)
        }
    }

    #[test]
    fn crc32_returns_a_shared_instance() {
        assert!(std::ptr::eq(Checksums::crc32(), Checksums::crc32()));
        assert_eq!(Checksums::crc32().compute(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn crc32c_returns_a_shared_instance() {
        assert!(std::ptr::eq(Checksums::crc32c(), Checksums::crc32c()));
        assert_eq!(Checksums::crc32c().compute(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn aliases_return_the_shared_crc32_instance() {
        for alias in [
            "CRC-32",
            "io.aeron.archive.checksum.Crc32",
            "org.agrona.checksum.Crc32",
        ] {
            assert!(std::ptr::eq(
                Checksums::crc32(),
                Checksums::new_instance(Some(alias)).unwrap()
            ));
        }
    }

    #[test]
    fn aliases_return_the_shared_crc32c_instance() {
        for alias in [
            "CRC-32C",
            "io.aeron.archive.checksum.Crc32c",
            "org.agrona.checksum.Crc32c",
        ] {
            assert!(std::ptr::eq(
                Checksums::crc32c(),
                Checksums::new_instance(Some(alias)).unwrap()
            ));
        }
    }

    #[test]
    fn empty_class_names_are_rejected() {
        assert!(matches!(
            Checksums::new_instance(None),
            Err(ChecksumError::EmptyClassName)
        ));
        assert!(matches!(
            Checksums::new_instance(Some("")),
            Err(ChecksumError::EmptyClassName)
        ));
    }

    #[test]
    fn unknown_class_names_are_rejected() {
        assert!(matches!(
            Checksums::new_instance(Some("a.b.c.MissingClass")),
            Err(ChecksumError::ClassNotFound(name)) if name == "a.b.c.MissingClass"
        ));
    }

    #[test]
    fn custom_checksums_are_constructed_as_distinct_instances() {
        let first = Checksums::new_instance_of::<TestChecksum>();
        let second = Checksums::new_instance_of::<TestChecksum>();
        assert!(!std::ptr::eq(first.as_ref(), second.as_ref()));
        assert_eq!(first.compute(&[]), 0);
        assert_eq!(second.compute(&[]), 0);
    }
}
