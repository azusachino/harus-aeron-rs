# Chapter 5: Media Driver Configuration & Publication Parameters

This chapter covers the media driver configuration defaults and `PublicationParams` URI parsing logic.

There's no exotic data structure to draw here — the property-name/default-value table in
`configuration.rs` *is* the chapter's table, and it's already exactly the shape you'd want it in:
a flat list of `(PROP_NAME, DEFAULT)` pairs like `MTU_LENGTH_PROP_NAME` / `MTU_LENGTH_DEFAULT =
1408` or `TERM_BUFFER_LENGTH_PROP_NAME` / `TERM_BUFFER_LENGTH_DEFAULT = 16 * 1024 * 1024`. The
useful framing isn't the table itself, it's what `PublicationParams` does with it:
`crates/driver/src/publication_params.rs` parses a channel URI exactly once per publication and
folds the result — URI overrides layered on top of the configuration defaults — into a single flat
`PublicationParams` struct (`linger_timeout_ns`, `term_length`, `mtu_length`,
`publication_window_length`, `initial_term_id`/`term_id`/`term_offset`, `session_id`, and so on).
Everything downstream in the driver — the log factory sizing a term buffer, the sender computing a
publication window — reads that struct once and never re-parses the URI. Treat `PublicationParams`
as derived, cached state, not a parser you re-run on every duty cycle.

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
