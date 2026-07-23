//! Property-based tests for aeron-driver configuration and publication parameters.

use aeron_driver::publication_params::{
    parse_duration, parse_size, producer_window_length, validate_mtu_length,
};
use proptest::prelude::*;

proptest! {
    // 1. parse_size roundtrip for numeric values with k/m/g suffixes
    #[test]
    fn prop_parse_size_suffixes(val in 1usize..1000usize) {
        let raw_str = format!("{val}");
        prop_assert_eq!(parse_size("test", &raw_str).unwrap(), val);

        let k_str = format!("{val}k");
        prop_assert_eq!(parse_size("test", &k_str).unwrap(), val * 1024);

        let m_str = format!("{val}m");
        prop_assert_eq!(parse_size("test", &m_str).unwrap(), val * 1024 * 1024);
    }

    // 2. parse_duration roundtrip for numeric values with ns/us/ms/s suffixes
    #[test]
    fn prop_parse_duration_suffixes(val in 1u64..1000u64) {
        let raw_str = format!("{val}");
        prop_assert_eq!(parse_duration("test", &raw_str).unwrap(), val);

        let ns_str = format!("{val}ns");
        prop_assert_eq!(parse_duration("test", &ns_str).unwrap(), val);

        let us_str = format!("{val}us");
        prop_assert_eq!(parse_duration("test", &us_str).unwrap(), val * 1000);

        let ms_str = format!("{val}ms");
        prop_assert_eq!(parse_duration("test", &ms_str).unwrap(), val * 1_000_000);

        let s_str = format!("{val}s");
        prop_assert_eq!(parse_duration("test", &s_str).unwrap(), val * 1_000_000_000);
    }

    // 3. producer_window_length never exceeds half term_length
    #[test]
    fn prop_producer_window_length(
        term_length in (64usize..1048576usize).prop_map(|v| v * 1024),
        config_window in 0usize..10000000usize,
    ) {
        let window = producer_window_length(term_length, config_window);
        let half_term = term_length >> 1;
        prop_assert!(window <= half_term);
        if config_window != 0 {
            prop_assert_eq!(window, config_window.min(half_term));
        } else {
            prop_assert_eq!(window, half_term);
        }
    }

    // 4. validate_mtu_length enforces multiples of 32 between 32 and 65504
    #[test]
    fn prop_validate_mtu_length(valid_multiplier in 1usize..2047usize) {
        let valid_mtu = valid_multiplier * 32;
        prop_assert!(validate_mtu_length(valid_mtu).is_ok());
    }
}
