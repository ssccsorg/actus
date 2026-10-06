//! Helpers that belong to no single layer.

/// Truncate `s` to at most `max` bytes without splitting a UTF-8 character. Returns the
/// original string when it is already short enough; otherwise the longest prefix that ends
/// on a char boundary.
///
/// It lives here rather than in the agent module it was written for, because the record
/// store truncates a message the same way and the store does not depend on the transport.
pub(crate) fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
