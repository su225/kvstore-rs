fn main() {
    println!("cargo:rustc-env=CARGO_PKG_AUTHORS={}", env!("CARGO_PKG_AUTHORS"));
    println!("cargo:rustc-env=CARGO_PKG_DESCRIPTION={}", env!("CARGO_PKG_DESCRIPTION"));
    println!("cargo:rustc-env=CARGO_PKG_VERSION={}", env!("CARGO_PKG_VERSION"));
}