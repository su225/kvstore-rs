fn main() {
    println!("cargo:rustc-env=CARGO_PKG_AUTHOR={}", env!("CARGO_PKG_AUTHORS"));
}