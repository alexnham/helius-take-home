//! One JSON object per line on stdout: `ts`, `level`, `event`, then the event's fields.
use serde_json::Value;

/// `fields` is a `json!({..})` object; `level` and `event` are plain identifiers.
pub fn log(level: &str, event: &str, fields: Value) {
    println!("{}", line(level, event, fields));
}

// Spliced by hand so ts, level and event lead the line; serde_json's map would sort them in among the fields.
fn line(level: &str, event: &str, fields: Value) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let fields = fields.to_string();
    let sep = if fields.len() > 2 { "," } else { "" };
    format!(r#"{{"ts":"{ts}","level":"{level}","event":"{event}"{sep}{}"#, &fields[1..])
}

#[test]
fn lines_are_json() {
    let with = line("info", "range_done", serde_json::json!({ "start": 1, "error": "a \"quoted\" msg" }));
    assert!(with.starts_with(r#"{"ts":""#));
    let parsed: Value = serde_json::from_str(&with).unwrap();
    assert_eq!((parsed["event"].as_str(), parsed["start"].as_i64()), (Some("range_done"), Some(1)));
    assert!(serde_json::from_str::<Value>(&line("warn", "stream_exit", serde_json::json!({}))).is_ok());
}
