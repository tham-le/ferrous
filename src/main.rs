//! `ferrous` CLI entry point.
//!
//! The CLI is intentionally minimal at this stage — just enough to wire the
//! binary to the library crate. Subcommand parsing is added in a later commit.

fn main() {
    println!("ferrous {}", ferrous::VERSION);
}
