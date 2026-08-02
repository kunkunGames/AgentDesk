//! Shared Discord image-attachment filename contract.
//!
//! Serenity constructs multipart MIME metadata from the filename supplied to
//! `CreateAttachment`. API validation and the durable outbox consumer must
//! therefore agree on the extension instead of trusting a separately stored
//! content type that the transport cannot apply.

const EXTENSION_MISMATCH: &str = "attachment filename extension must match its image content type";
const UNSUPPORTED_CONTENT_TYPE: &str =
    "attachment content type is not a supported Discord image type";

pub(crate) fn validate_filename_content_type(
    filename: &str,
    content_type: &str,
) -> Result<(), &'static str> {
    let filename = filename.trim().to_ascii_lowercase();
    let valid = match content_type.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" => filename.ends_with(".jpg") || filename.ends_with(".jpeg"),
        "image/png" => filename.ends_with(".png"),
        "image/webp" => filename.ends_with(".webp"),
        "image/gif" => filename.ends_with(".gif"),
        _ => return Err(UNSUPPORTED_CONTENT_TYPE),
    };
    valid.then_some(()).ok_or(EXTENSION_MISMATCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_and_jpeg_alias_extensions_case_insensitively() {
        for (filename, content_type) in [
            ("thumbnail.PNG", "image/png"),
            ("thumbnail.jpg", "image/jpeg"),
            ("thumbnail.JPEG", "IMAGE/JPEG"),
            ("thumbnail.webp", "image/webp"),
            ("thumbnail.gif", "image/gif"),
        ] {
            validate_filename_content_type(filename, content_type)
                .unwrap_or_else(|error| panic!("{filename} / {content_type}: {error}"));
        }
    }

    #[test]
    fn rejects_missing_mismatched_and_unsupported_extensions() {
        for (filename, content_type) in [
            ("thumbnail", "image/png"),
            ("thumbnail.jpg", "image/png"),
            ("thumbnail.png", "image/jpeg"),
            ("thumbnail.png", "text/plain"),
        ] {
            assert!(
                validate_filename_content_type(filename, content_type).is_err(),
                "{filename} / {content_type} must be rejected"
            );
        }
    }
}
