//! Standalone Archive process attached to an already-running Media Driver.

use std::io::{Read, Write};
use std::path::PathBuf;

use aeron_archive::service::{ArchiveContext, ManagedArchive};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os();
    let _program = args.next();
    let aeron_directory = args.next().map(PathBuf::from).ok_or_else(usage)?;
    let archive_directory = args.next().map(PathBuf::from).ok_or_else(usage)?;
    if args.next().is_some() {
        return Err(usage().into());
    }

    let archive = ManagedArchive::start(ArchiveContext::new(
        aeron_directory,
        archive_directory.clone(),
    ))?;
    println!("{}", archive_directory.display());
    std::io::stdout().flush()?;

    let mut release = String::new();
    std::io::stdin().read_to_string(&mut release)?;
    archive.stop()?;
    Ok(())
}

fn usage() -> String {
    "usage: aeron-archive <aeron-directory> <archive-directory>".into()
}
