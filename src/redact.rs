use std::io::{self, Write};
use std::sync::Arc;
use tracing_subscriber::fmt::MakeWriter;

/// Replaces known secrets and sensitive query parameter values in log output.
#[derive(Clone, Default)]
pub struct Redactor {
    secrets: Arc<Vec<String>>,
}

impl Redactor {
    pub fn new<I>(secrets: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let mut secrets: Vec<String> = secrets
            .into_iter()
            .filter(|secret| !secret.is_empty())
            .collect();
        secrets.sort();
        secrets.dedup();
        // Replace longer secrets first so one secret cannot partially mask another.
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        Self {
            secrets: Arc::new(secrets),
        }
    }

    pub fn redact(&self, text: &str) -> String {
        let mut output = text.to_owned();
        for secret in self.secrets.iter() {
            if output.contains(secret) {
                output = output.replace(secret, "[REDACTED]");
            }
        }
        redact_sensitive_params(&output)
    }

    pub fn redact_bytes(&self, bytes: &[u8]) -> Vec<u8> {
        let text = String::from_utf8_lossy(bytes);
        self.redact(&text).into_bytes()
    }
}

const SENSITIVE_PARAMS: &[&str] = &[
    "access_token",
    "client_secret",
    "api_key",
    "apikey",
    "password",
    "passwd",
    "private_key",
    "secret",
    "token",
    "key",
];

fn redact_sensitive_params(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut ranges = Vec::new();

    for name in SENSITIVE_PARAMS {
        // JSON style: `"name": "value"`.
        let json_needle = format!("\"{name}\"");
        for (index, _) in lower.match_indices(&json_needle) {
            push_value(text, index + json_needle.len(), &mut ranges);
        }

        // Query/form, header, and log style: `name=value` or `name: value`.
        for (index, _) in lower.match_indices(name) {
            if !boundary_before(&lower, index) {
                continue;
            }
            push_value(text, index + name.len(), &mut ranges);
        }
    }

    // Authorization headers often contain a bearer token without a name=value pair.
    for (index, _) in lower.match_indices("bearer ") {
        let start = skip_whitespace(text, index + "bearer ".len());
        push_delimited(text, start, None, &mut ranges);
    }

    if ranges.is_empty() {
        return text.to_owned();
    }

    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
            continue;
        }
        merged.push((start, end));
    }

    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end) in merged {
        output.push_str(&text[cursor..start]);
        output.push_str("[REDACTED]");
        cursor = end;
    }
    output.push_str(&text[cursor..]);
    output
}

fn boundary_before(text: &str, index: usize) -> bool {
    match text[..index].chars().next_back() {
        Some(previous) => !(previous.is_ascii_alphanumeric() || previous == '_'),
        None => true,
    }
}

fn skip_whitespace(text: &str, mut cursor: usize) -> usize {
    while let Some(character) = text[cursor..].chars().next() {
        if !character.is_whitespace() {
            break;
        }
        cursor += character.len_utf8();
    }
    cursor
}

/// Extracts the value after a `=` or `:` separator, if one is present.
fn push_value(text: &str, after_name: usize, ranges: &mut Vec<(usize, usize)>) {
    let cursor = skip_whitespace(text, after_name);
    let Some(separator) = text[cursor..].chars().next() else {
        return;
    };
    if separator != '=' && separator != ':' {
        return;
    }
    let cursor = skip_whitespace(text, cursor + separator.len_utf8());
    push_delimited(text, cursor, None, ranges);
}

/// Pushes the range of a value, optionally quoted, stopping at the matching
/// quote or the first delimiter.
fn push_delimited(
    text: &str,
    cursor: usize,
    quote: Option<char>,
    ranges: &mut Vec<(usize, usize)>,
) {
    let cursor = skip_whitespace(text, cursor);
    let quote = quote.or_else(|| {
        text[cursor..]
            .chars()
            .next()
            .filter(|character| *character == '"' || *character == '\'')
    });
    let start = cursor + quote.map_or(0, char::len_utf8);
    let mut end = start;
    let mut escaped = false;
    for (offset, character) in text[start..].char_indices() {
        if let Some(quote) = quote {
            if escaped {
                escaped = false;
                end = start + offset + character.len_utf8();
                continue;
            }
            if character == '\\' {
                escaped = true;
                end = start + offset + character.len_utf8();
                continue;
            }
            if character == quote {
                break;
            }
        } else if matches!(
            character,
            '&' | ' ' | '\t' | '\n' | '\r' | '"' | '\'' | ')' | ']' | '}' | ',' | ';' | '<' | '>'
        ) {
            break;
        }
        end = start + offset + character.len_utf8();
    }
    if end > start {
        ranges.push((start, end));
    }
}

/// A writer that buffers complete lines and redacts them before writing.
pub struct RedactingWriter<W: Write> {
    inner: W,
    redactor: Redactor,
    buffer: Vec<u8>,
}

impl<W: Write> RedactingWriter<W> {
    fn new(inner: W, redactor: Redactor) -> Self {
        Self {
            inner,
            redactor,
            buffer: Vec::new(),
        }
    }

    fn write_redacted(&mut self, line: &[u8]) -> io::Result<()> {
        self.inner.write_all(&self.redactor.redact_bytes(line))
    }
}

impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=position).collect();
            self.write_redacted(&line)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.write_redacted(&line)?;
        }
        self.inner.flush()
    }
}

#[derive(Clone)]
pub struct RedactingMakeWriter {
    redactor: Redactor,
}

impl RedactingMakeWriter {
    pub fn new(redactor: Redactor) -> Self {
        Self { redactor }
    }
}

impl<'a> MakeWriter<'a> for RedactingMakeWriter {
    type Writer = RedactingWriter<std::io::Stdout>;

    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter::new(std::io::stdout(), self.redactor.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_registered_secrets() {
        let redactor = Redactor::new(vec!["super-secret-key".to_owned()]);
        let output = redactor.redact("request failed for super-secret-key");
        assert_eq!(output, "request failed for [REDACTED]");
    }

    #[test]
    fn redacts_sensitive_query_parameters() {
        let redactor = Redactor::default();
        let output = redactor.redact("GET /x?key=abc123&password=hunter2&name=ok");
        assert_eq!(output, "GET /x?key=[REDACTED]&password=[REDACTED]&name=ok");
    }

    #[test]
    fn does_not_redact_words_containing_key() {
        let redactor = Redactor::default();
        assert_eq!(redactor.redact("monkey=banana"), "monkey=banana");
    }

    #[test]
    fn redacts_quoted_json_and_header_values() {
        let redactor = Redactor::default();
        assert_eq!(
            redactor.redact(r#"{"key":"abc","name":"ok"}"#),
            r#"{"key":"[REDACTED]","name":"ok"}"#
        );
        assert_eq!(
            redactor.redact("Authorization: Bearer abc123"),
            "Authorization: Bearer [REDACTED]"
        );
        assert_eq!(redactor.redact(r#"key="abc""#), r#"key="[REDACTED]""#);
        assert_eq!(
            redactor.redact("password: hunter2\n"),
            "password: [REDACTED]\n"
        );
    }

    #[test]
    fn redacts_multiple_secrets_and_bytes() {
        let redactor = Redactor::new(vec!["alpha".to_owned(), "beta".to_owned()]);
        assert_eq!(redactor.redact("alpha beta"), "[REDACTED] [REDACTED]");
        assert_eq!(
            String::from_utf8(redactor.redact_bytes(b"alpha\n")).unwrap(),
            "[REDACTED]\n"
        );
    }

    #[test]
    fn writer_redacts_line_by_line() {
        let redactor = Redactor::new(vec!["secret-value".to_owned()]);
        let mut writer = RedactingWriter::new(Vec::new(), redactor);
        writer
            .write_all(b"first secret-value\nsecond normal\n")
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(
            String::from_utf8(writer.inner).unwrap(),
            "first [REDACTED]\nsecond normal\n"
        );
    }

    #[test]
    fn writer_handles_secret_split_across_writes() {
        let redactor = Redactor::new(vec!["secret-value".to_owned()]);
        let mut writer = RedactingWriter::new(Vec::new(), redactor);
        writer.write_all(b"before secret-").unwrap();
        writer.write_all(b"value after\n").unwrap();
        writer.flush().unwrap();
        assert_eq!(
            String::from_utf8(writer.inner).unwrap(),
            "before [REDACTED] after\n"
        );
    }

    #[test]
    fn writer_redacts_unterminated_buffer_on_flush() {
        let redactor = Redactor::new(vec!["secret-value".to_owned()]);
        let mut writer = RedactingWriter::new(Vec::new(), redactor);
        writer.write_all(b"secret-value").unwrap();
        writer.flush().unwrap();
        assert_eq!(String::from_utf8(writer.inner).unwrap(), "[REDACTED]");
    }
}
