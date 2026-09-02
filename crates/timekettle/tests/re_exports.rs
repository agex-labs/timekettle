#[test]
fn facade_schema_version_matches_record_crate() {
    assert_eq!(
        timekettle::CURRENT_EVENT_SCHEMA_VERSION,
        timekettle_runtime::CURRENT_EVENT_SCHEMA_VERSION,
        "the public timekettle facade must expose the same event schema version that timekettle-runtime writes"
    );
}
