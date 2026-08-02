//! Shared Discord image-attachment filename contract.
//!
//! Serenity constructs multipart MIME metadata from the filename supplied to
//! `CreateAttachment`. New API writes therefore require a matching extension,
//! while the durable consumer repairs filenames written before that contract
//! existed so an upgrade cannot strand already-reserved deliveries.

use std::borrow::Cow;

const EXTENSION_MISMATCH: &str = "attachment filename extension must match its image content type";
const UNSUPPORTED_CONTENT_TYPE: &str =
    "attachment content type is not a supported Discord image type";

fn content_type_extensions(
    content_type: &str,
) -> Result<(&'static str, &'static [&'static str]), &'static str> {
    match content_type.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" => Ok(("jpg", &["jpg", "jpeg"])),
        "image/png" => Ok(("png", &["png"])),
        "image/webp" => Ok(("webp", &["webp"])),
        "image/gif" => Ok(("gif", &["gif"])),
        _ => Err(UNSUPPORTED_CONTENT_TYPE),
    }
}

fn filename_has_extension(filename: &str, extensions: &[&str]) -> bool {
    let filename = filename.trim();
    let Some((_, extension)) = filename.rsplit_once('.') else {
        return false;
    };
    extensions
        .iter()
        .any(|expected| extension.eq_ignore_ascii_case(expected))
}

pub(crate) fn validate_filename_content_type(
    filename: &str,
    content_type: &str,
) -> Result<(), &'static str> {
    let (_, extensions) = content_type_extensions(content_type)?;
    filename_has_extension(filename, extensions)
        .then_some(())
        .ok_or(EXTENSION_MISMATCH)
}

/// Return the transport filename for a durable image attachment.
///
/// Migration 0103 initially accepted plain or MIME-mismatched filenames. Those
/// rows are valid legacy reservations because their MIME and file signatures
/// were still checked. Preserve the reservation and repair only its outbound
/// filename; unsupported MIME values remain fail-closed.
pub(crate) fn delivery_filename<'a>(
    filename: &'a str,
    content_type: &str,
) -> Result<Cow<'a, str>, &'static str> {
    let (canonical_extension, accepted_extensions) = content_type_extensions(content_type)?;
    if filename_has_extension(filename, accepted_extensions) {
        return Ok(Cow::Borrowed(filename));
    }

    let trimmed = filename.trim();
    let stem = match trimmed.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => stem,
        Some((_, _)) => "attachment",
        None => trimmed,
    }
    .trim_end_matches('.');
    let stem = if stem.is_empty() { "attachment" } else { stem };
    Ok(Cow::Owned(format!("{stem}.{canonical_extension}")))
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

    #[test]
    fn normalizes_legacy_filenames_without_changing_valid_names() {
        assert_eq!(
            delivery_filename("thumbnail", "image/png").unwrap(),
            "thumbnail.png"
        );
        assert_eq!(
            delivery_filename("thumbnail.jpg", "image/png").unwrap(),
            "thumbnail.png"
        );
        assert_eq!(
            delivery_filename("family.photo.jpeg", "image/jpeg").unwrap(),
            "family.photo.jpeg"
        );
        assert_eq!(
            delivery_filename(".legacy", "image/gif").unwrap(),
            "attachment.gif"
        );
        assert!(delivery_filename("thumbnail.png", "text/plain").is_err());
    }
}
