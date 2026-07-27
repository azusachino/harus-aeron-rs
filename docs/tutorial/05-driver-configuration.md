# Chapter 5: Media Driver Configuration & Publication Parameters

This chapter covers the media driver configuration defaults and `PublicationParams` URI parsing logic.

## Configuration Defaults

`crates/driver/src/configuration.rs` mirrors upstream `io.aeron.driver.Configuration.java`. It defines:
- Property name constants (`TERM_BUFFER_SPARSE_FILE_PROP_NAME`, `MTU_LENGTH_PROP_NAME`, `PUBLICATION_LINGER_PROP_NAME`, etc.).
- Default values (`MTU_LENGTH_DEFAULT = 1408`, `TERM_BUFFER_LENGTH_DEFAULT = 16MB`, `MAX_RESEND_DEFAULT = 16`, etc.).
- Environment variable property loading via thread-safe closures to allow parallel tests without environment variable races.

## Publication Parameters

`crates/driver/src/publication_params.rs` handles URI parameter parsing and validation:
- MTU length and term length bounds checking.
- Publication window length calculations (`pub-wnd`).
- Linger timeouts and untethered subscription timeouts (`untethered-window-limit-timeout`, `untethered-resting-timeout`, `untethered-linger-timeout`).
- Tagged session resolution via the `TaggedPublicationLookup` trait.
- Term triple validation (`initial-term-id`, `term-id`, `term-offset`).

## Verification

Run all tests and workspace checks with:

```bash
make check
```
