#![no_main]

use aeron_client::uri::ChannelUri;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let uri = String::from_utf8_lossy(data);
    let Ok(parsed) = ChannelUri::parse(&uri) else {
        return;
    };

    let rendered = parsed.to_string();
    let reparsed = ChannelUri::parse(&rendered).expect("display output must remain parseable");
    assert_eq!(reparsed, parsed);
});
