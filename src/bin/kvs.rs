use clap::{arg, Command};
use std::process::exit;

fn main() {
    let matches = Command::new("kvstore-rs")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to the string")
                .arg(arg!(<KEY> "The key to set"))
                .arg(arg!(<VALUE> "The value to set")),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<KEY> "the key to get")),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a given key")
                .arg(arg!(<KEY> "The key to remove")),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_m)) => {
            let _key = sub_m.get_one::<String>("KEY").unwrap();
            let _value = sub_m.get_one::<String>("VALUE").unwrap();
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("get", sub_m)) => {
            let _key = sub_m.get_one::<String>("KEY").unwrap();
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("rm", sub_m)) => {
            let _key = sub_m.get_one::<String>("KEY").unwrap();
            eprintln!("unimplemented");
            exit(1);
        }
        _ => {
            eprintln!("Use {} -h for help", env!("CARGO_PKG_NAME"));
            exit(1);
        }
    }
}
