fn main() {
    let raw = "startup_doctor_failed:2";
    let parts: Vec<&str> = raw.split(':').collect();
    println!("{:?}", parts);
}
