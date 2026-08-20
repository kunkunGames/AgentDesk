fn dollar_quote_delimiter(sql: &str, start: usize) -> Option<String> {
    let bytes = sql.as_bytes();
    if start >= bytes.len() || bytes[start] != b'$' {
        return None;
    }
    let mut idx = start + 1;
    while idx < bytes.len() && bytes[idx] != b'$' {
        if !bytes[idx].is_ascii_alphabetic() && bytes[idx] != b'_' {
            return None;
        }
        idx += 1;
    }
    if idx < bytes.len() && bytes[idx] == b'$' {
        Some(sql[start..=idx].to_string())
    } else {
        None
    }
}

fn scan_double_quoted_identifier(sql: &str, start: usize) -> Option<(&str, &str, usize)> {
    let bytes = sql.as_bytes();
    if start >= bytes.len() || bytes[start] != b'"' {
        return None;
    }
    let mut idx = start + 1;
    while idx < bytes.len() {
        if bytes[idx] == b'"' {
            if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                idx += 2;
                continue;
            }
            return Some((
                &sql[start..idx + 1],
                &sql[start + 1..idx],
                idx + 1,
            ));
        }
        idx += 1;
    }
    None
}

fn skip_ws_and_comments_backward(sql: &str, target: usize) -> Option<usize> {
    let mut idx = 0;
    let bytes = sql.as_bytes();
    let mut last_valid_end = None;

    while idx < target {
        if bytes[idx].is_ascii_whitespace() {
            idx += 1;
            continue;
        }

        if sql[idx..].starts_with("--") {
            let line_end = sql[idx..].find('\n').map(|p| idx + p + 1).unwrap_or(sql.len());
            idx = line_end;
            continue;
        }

        if sql[idx..].starts_with("/*") {
            let block_end = sql[idx..].find("*/").map(|p| idx + p + 2).unwrap_or(sql.len());
            idx = block_end;
            continue;
        }

        if bytes[idx] == b'\'' {
            let mut end_idx = idx + 1;
            while end_idx < sql.len() {
                if bytes[end_idx] == b'\'' {
                    end_idx += 1;
                    if end_idx < sql.len() && bytes[end_idx] == b'\'' {
                        end_idx += 1; // escaped quote
                    } else {
                        break;
                    }
                } else {
                    end_idx += 1;
                }
            }
            last_valid_end = Some(end_idx.min(target));
            idx = end_idx;
            continue;
        }

        if bytes[idx] == b'"' {
            if let Some((_, _, next_idx)) = scan_double_quoted_identifier(sql, idx) {
                last_valid_end = Some(next_idx.min(target));
                idx = next_idx;
            } else {
                idx += 1;
                last_valid_end = Some(idx.min(target));
            }
            continue;
        }

        if let Some(delimiter) = dollar_quote_delimiter(sql, idx) {
            let end_search_start = idx + delimiter.len();
            if let Some(end_rel) = sql[end_search_start..].find(&delimiter) {
                let end_idx = end_search_start + end_rel + delimiter.len();
                last_valid_end = Some(end_idx.min(target));
                idx = end_idx;
            } else {
                idx = sql.len();
                last_valid_end = Some(idx.min(target));
            }
            continue;
        }

        if bytes[idx].is_ascii_alphabetic() || bytes[idx] == b'_' {
            let mut end_idx = idx + 1;
            while end_idx < sql.len()
                && (bytes[end_idx].is_ascii_alphanumeric()
                    || bytes[end_idx] == b'_'
                    || bytes[end_idx] == b'.')
            {
                end_idx += 1;
            }
            last_valid_end = Some(end_idx.min(target));
            idx = end_idx;
            continue;
        }

        let char_len = sql[idx..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        idx += char_len;
        last_valid_end = Some(idx.min(target));
    }

    last_valid_end
}

fn main() {
    let sql = "SELECT 識別子, rowid FROM t";
    //           0123456...
    let res = skip_ws_and_comments_backward(sql, 24);
    println!("{:?}", res);
}
