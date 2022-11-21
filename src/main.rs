mod client;
mod encoder;
mod parser;
mod types;
mod storage;

use clap::{value_parser, Arg, Command};
use lunatic::{net::TcpListener, process::StartProcess, Mailbox, ProcessConfig};
use lunatic_log::{info, subscriber::fmt::FmtSubscriber, LevelFilter};

use crate::{client::ClientProcess, storage::Storage};

#[lunatic::main]
fn main(_: Mailbox<()>) {
    let (addr, log_level) = parse_args();
    lunatic_log::init(FmtSubscriber::new(log_level).pretty());

    Storage::start_link((), Some("storage"));

    info!("Listening to: {addr}");
    let listener = TcpListener::bind(addr).unwrap();
    let mut client_conf = ProcessConfig::new().unwrap();
    client_conf.set_max_memory(5_000_000);
    client_conf.set_can_spawn_processes(true);

    while let Ok((stream, _)) = listener.accept() {
        ClientProcess::start_config(stream, None, &client_conf);
    }
}

fn parse_args() -> (String, LevelFilter) {
    let matches = Command::new("moonis")
        .version("0.1")
        .author("Roger")
        .about("An implementation of redis using lunatic")
        .arg(
            Arg::new("ADDR")
                .default_value("127.0.0.1")
                .short('a')
                .long("address")
                .help("Sets the listening addr for the server"),
        )
        .arg(
            Arg::new("PORT")
                .value_parser(value_parser!(u16).range(1..65535))
                .default_value("6142")
                .short('p')
                .long("port")
                .help("Sets the listening port for the server"),
        )
        .arg(
            Arg::new("LOG_LEVEL")
                .value_parser(value_parser!(LevelFilter))
                .default_value("info")
                .short('l')
                .long("log_level")
                .help("Sets the log level"),
        )
        .get_matches();
    let addr = matches.get_one::<String>("ADDR").unwrap();
    let port = matches.get_one::<u16>("PORT").unwrap();
    let log_level = matches.get_one::<LevelFilter>("LOG_LEVEL").unwrap();
    (format!("{addr}:{port}"), log_level.to_owned())
}
