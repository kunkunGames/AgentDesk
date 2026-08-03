use regex::Regex;

fn main() {
    let re = Regex::new(
        r"(?i)\b((?:authorization|cookie|set-cookie)[ \t]*:[ \t]*(?:[a-z][a-z0-9._~+/-]*[ \t]+)?)(?:[^\r\n]+(?:\r?\n[ \t]+[^\r\n]+)*|(?:\r?\n[ \t]+[^\r\n]+)+)",
    ).unwrap();
    let text = "Cookie: session=12345\nNext line";
    let redacted = re.replace_all(text, "$1***");
    println!("{}", redacted);
}
