//! Read-only Archive catalog inspection tool.

use aeron_archive::archive_tool::run;

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();
    if let Err(error) = run(&args, &mut std::io::stdout()) {
        eprintln!("ERROR - {error}");
        std::process::exit(1);
    }
}
