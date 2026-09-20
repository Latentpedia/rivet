//! The LadybugDB server process for the ladybug-graph platform.
//!
//! Owns the graph file (it is the only process allowed to open it) and serves the columnar ADBC
//! RPC protocol to any number of remote worker and coordinator processes. Workers never touch the
//! file; they connect over HTTP and stream Arrow result sets, so the platform distributes across
//! machines while the embedded engine keeps its single-writer guarantee.
//!
//! Usage:
//!
//! ```text
//! ladybug-server [--db <path|:memory:>] [--listen <addr:port>]
//! ```
//!
//! Defaults to `--db :memory: --listen 127.0.0.1:8123`.

use anyhow::{Context, Result, bail};
use tracing::info;

use example_ladybug_graph::ladybug_server::{LadybugServer, install_local_hooks};

fn init_logging() {
	let filter = tracing_subscriber::EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
	let _ = tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_writer(std::io::stderr)
		.try_init();
}

fn usage() -> &'static str {
	"usage: ladybug-server [--db <path|:memory:>] [--listen <addr:port>]"
}

fn main() -> Result<()> {
	init_logging();
	let mut db = ":memory:".to_owned();
	let mut listen = "127.0.0.1:8123".to_owned();
	let mut args = std::env::args().skip(1);
	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--db" => db = args.next().context("--db requires a path")?,
			"--listen" => listen = args.next().context("--listen requires an address")?,
			"--help" | "-h" => {
				println!("{}", usage());
				return Ok(());
			}
			other => bail!("unknown argument `{other}`; {}", usage()),
		}
	}

	// Real engine hooks, installed before the store opens and held to process exit so
	// they outlive every Database. Declared first so it drops last.
	let _hooks = install_local_hooks()?;
	let server = LadybugServer::open(&db)?;
	let addr: std::net::SocketAddr = listen.parse().context("invalid --listen address")?;
	let rt = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.context("build ladybug server runtime")?;
	rt.block_on(async move {
		let listener = tokio::net::TcpListener::bind(addr)
			.await
			.context("bind listen address")?;
		info!(%addr, db, "ladybug server listening (single writer owns the store)");
		axum::serve(listener, server.app())
			.await
			.context("serve ladybug rpc")
	})
}
