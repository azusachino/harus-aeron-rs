# Fuzzing

The standalone [`fuzz/`](https://github.com/azusachino/harus-aeron-rs/tree/main/fuzz) package uses `libfuzzer-sys` and stays outside
the main Cargo workspace. It has four deliberately small targets:

- `frame_decode` exercises fixed-size Aeron DATA, ERROR, STATUS, NAK, SETUP, and
  RTTM flyweights with arbitrary bytes;
- `uri_parse` checks that successfully parsed URIs survive display and parse
  round-trips;
- `ring_buffer_read` reads arbitrary record headers from a bounded MPSC ring
  buffer view;
- `log_buffer_read` feeds arbitrary term data to `TermReader`.

Compile and format-check the targets without installing cargo-fuzz:

```bash
make fuzz-check
```

Run the targets with the standard Rust fuzzing tool for ten seconds each:

```bash
make fuzz
FUZZ_TIME=60 make fuzz
```

`cargo-fuzz` owns the sanitizer/fuzzer build flags. The target inputs are
intentionally untrusted; a panic or sanitizer finding is a regression to
minimize and add to the relevant target's corpus before changing parser behavior.
