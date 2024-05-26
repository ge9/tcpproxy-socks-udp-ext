use futures::FutureExt;
use getopts::Options;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use tokio::net::UdpSocket;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::process::Command;

type BoxedError = Box<dyn std::error::Error + Sync + Send + 'static>;
static DEBUG: AtomicBool = AtomicBool::new(false);
const BUF_SIZE: usize = 1024;

fn print_usage(program: &str, opts: Options) {
    let program_path = std::path::PathBuf::from(program);
    let program_name = program_path.file_stem().unwrap().to_string_lossy();
    let brief = format!(
        "Usage: {} REMOTE_HOST:PORT [-b BIND_ADDR] [-l LOCAL_PORT] [-c COMMAND] ...",
        program_name
    );
    print!("{}", opts.usage(&brief));
}

#[tokio::main]
async fn main() -> Result<(), BoxedError> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "b",
        "bind",
        "The address on which to listen for incoming requests, defaulting to localhost",
        "BIND_ADDR",
    );
    opts.optopt(
        "l",
        "local-port",
        "The local port to which tcpproxy should bind to, randomly chosen otherwise",
        "LOCAL_PORT",
    );
    opts.optmulti(
        "c",
        "command",
        "The command to call remote helper. Use this option multiple time for complex commands.",
        "COMMAND",
    );
    opts.optflag("d", "debug", "Enable debug mode");

    let matches = match opts.parse(&args[1..]) {
        Ok(opts) => opts,
        Err(e) => {
            eprintln!("{}", e);
            print_usage(&program, opts);
            std::process::exit(-1);
        }
    };
    let remote = match matches.free.len() {
        1 => matches.free[0].clone(),
        _ => {
            print_usage(&program, opts);
            std::process::exit(-1);
        }
    };

    if !remote.contains(':') {
        eprintln!("A remote port is required (REMOTE_ADDR:PORT)");
        std::process::exit(-1);
    }

    DEBUG.store(matches.opt_present("d"), Ordering::Relaxed);
    // let local_port: i32 = matches.opt_str("l").unwrap_or("0".to_string()).parse()?;
    let local_port: i32 = matches.opt_str("l").map(|s| s.parse()).unwrap_or(Ok(0))?;
    let bind_addr = match matches.opt_str("b") {
        Some(addr) => addr,
        None => "127.0.0.1".to_owned(),
    };
    let ssh_command = matches.opt_strs("c");
    forward(&bind_addr, local_port, remote, ssh_command).await
}

async fn read_from_stdout(
    stdout_reader0: Arc<tokio::sync::Mutex<tokio::process::ChildStdout>>,
    conmap: Arc<tokio::sync::Mutex<HashMap<(u8, u8, u8, u8, u8, u8), Arc<UdpSocket>>>>,
) {
    let mut stdout = stdout_reader0.lock().await;
    let mut buffer = [0; 8];
    loop {
        stdout.read_exact(&mut buffer).await.expect("error reading header");

        let length = u16::from_be_bytes([buffer[6], buffer[7]]);
        let mut data = vec![0; length as usize];
        stdout.read_exact(&mut data).await.expect("error reading data");
        if let Some(u) = conmap.lock().await.get(&(
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5],
        )) {
            u.send(&data).await.unwrap();
        }
    }
}

async fn forward(bind_ip: &str, local_port: i32, remote: String, command: Vec<String>) -> Result<(), BoxedError> {
    let command_slice = command.as_slice();
    let mut child = Command::new(&command_slice[0])
        .args(&command_slice[1..])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to start command");
    let stdin = child.stdin.take().expect("Failed to open stdin");
    let stdout = child.stdout.take().expect("Failed to open stdout");
    let stdin_writer = Arc::new(tokio::sync::Mutex::new(stdin));
    let stdout_reader = Arc::new(tokio::sync::Mutex::new(stdout));
    let conmap = HashMap::new();
    let conmap0 = Arc::new(tokio::sync::Mutex::new(conmap));
    let conmap2 = Arc::clone(&conmap0);
    let stdout_reader2 = Arc::clone(&stdout_reader);
   
    tokio::spawn(async move {
        read_from_stdout( stdout_reader2, conmap2).await
    });
    // Listen on the specified IP and port
    let bind_addr = if !bind_ip.starts_with('[') && bind_ip.contains(':') {
        // Correctly format for IPv6 usage
        format!("[{}]:{}", bind_ip, local_port)
    } else {
        format!("{}:{}", bind_ip, local_port)
    };
    let bind_sock = bind_addr
        .parse::<std::net::SocketAddr>()
        .expect("Failed to parse bind address");
    let listener = TcpListener::bind(&bind_sock).await?;
    println!("Listening on {}", listener.local_addr().unwrap());

    // `remote` should be either the host name or ip address, with the port appended.
    // It doesn't get tested/validated until we get our first connection, though!

    // We leak `remote` instead of wrapping it in an Arc to share it with future tasks since
    // `remote` is going to live for the lifetime of the server in all cases.
    // (This reduces MESI/MOESI cache traffic between CPU cores.)
    let remote: &str = Box::leak(remote.into_boxed_str());

    // Two instances of this function are spawned for each half of the connection: client-to-server,
    // server-to-client. We can't use tokio::io::copy() instead (no matter how convenient it might
    // be) because it doesn't give us a way to correlate the lifetimes of the two tcp read/write
    // loops: even after the client disconnects, tokio would keep the upstream connection to the
    // server alive until the connection's max client idle timeout is reached.
    async fn copy_with_abort<R, W>(
        read: &mut R,
        write: &mut W,
        mut abort: broadcast::Receiver<()>,
        r2c: bool, //remote to local
        state: Arc<std::sync::Mutex<bool>>,
        stdin_writer: Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>,
        conmap: Arc<tokio::sync::Mutex<HashMap<(u8, u8, u8, u8, u8, u8), Arc<UdpSocket>>>>, //UdpSocket doesn't require Mutex
        local_addr: std::net::IpAddr
    ) -> tokio::io::Result<usize>
    where
        R: tokio::io::AsyncRead + Unpin,
        W: tokio::io::AsyncWrite + Unpin,
    {
        let mut copied = 0;
        let mut buf = [0u8; BUF_SIZE];
        let mut sv_tuple = (0,0,0,0,0,0);
        let udp_closed = Arc::new(std::sync::Mutex::new(false));
        loop {
            let udp_closed1 = Arc::clone(&udp_closed);
            let bytes_read;
            //TODO: We should use read_exact to parse byte stream correctly in any situations.
            tokio::select! {
                biased;
                result = read.read(&mut buf) => {
                    use std::io::ErrorKind::{ConnectionReset, ConnectionAborted};
                    bytes_read = result.or_else(|e| match e.kind() {
                        // Consider these to be part of the proxy life, not errors
                        ConnectionReset | ConnectionAborted => Ok(0),
                        _ => Err(e)
                    })?;
                },
                _ = abort.recv() => {
                    break;
                }
            }

            if bytes_read == 0 {
                break;
            }

            if *state.lock().unwrap() {
                //TODO: support IPv6
                sv_tuple = (buf[4], buf[5], buf[6], buf[7], buf[8], buf[9]);
                let stdin_writer1 = Arc::clone(&stdin_writer);
                tokio::spawn(async move {
                    let mut stdin = stdin_writer1.lock().await;
                    //request connection to the remote UDP port
                    stdin.write_all(&[buf[4], buf[5], buf[6], buf[7], 0, 0, buf[8], buf[9], 0]).await.unwrap();
                    stdin.flush().await.unwrap();
                });
                let stdin_writer2 = Arc::clone(&stdin_writer);
                let new_socket = tokio::task::block_in_place(|| async {
                    let udp_socket = UdpSocket::bind((local_addr,0)).await.unwrap();
                    let udp_port = udp_socket.local_addr().unwrap().port();
                    let mutsocket = Arc::new(udp_socket);
                    eprintln!("[client-info] Assigned {} for {:?}", udp_port, sv_tuple);
                    conmap.lock().await.insert(sv_tuple, Arc::clone(&mutsocket));
                    tokio::spawn(async move {
                        let mut buf = [0; 1502];
                        {
                            let (size, src) = mutsocket.recv_from(&mut buf).await.expect("error in receiving first packet");
                            mutsocket.connect(src).await.unwrap();
                            let s = u16::to_be_bytes(size as u16);
                            eprintln!("[client-info] Received the first UDP packet from {}, connected", src);
                            let stdin_writer = Arc::clone(&stdin_writer2);
                            let mut stdin = stdin_writer.lock().await;
                            stdin.write_all(&[sv_tuple.0, sv_tuple.1, sv_tuple.2, sv_tuple.3,sv_tuple.4, sv_tuple.5, s[0], s[1]]).await.unwrap();
                            stdin.write_all(&buf[..size]).await.unwrap();
                            stdin.flush().await.unwrap();
                        }
                        while !*udp_closed1.lock().unwrap() {
                            let (size, _) = mutsocket.recv_from(&mut buf).await.expect("error in receiving packet");
                            let s = u16::to_be_bytes(size as u16);
                            let stdin_writer = Arc::clone(&stdin_writer2);
                            let mut stdin = stdin_writer.lock().await;
                            stdin.write_all(&[sv_tuple.0, sv_tuple.1, sv_tuple.2, sv_tuple.3,sv_tuple.4, sv_tuple.5, s[0], s[1]]).await.unwrap();
                            stdin.write_all( &buf[..size]).await.unwrap();
                            stdin.flush().await.unwrap();
                        }
                    });
                    return udp_port;
                }).await;
                let a = new_socket.to_be_bytes();
                buf[4] = 127;buf[5] = 0;buf[6] = 0;buf[7] = 1;buf[8] = a[0];buf[9] = a[1]
            }
            {
                let mut s = state.lock().unwrap();
                *s =  !r2c && buf[0] == 5 && buf[1] == 3;
            }
            // While we ignore some read errors above, any error writing data we've already read to
            // the other side is always treated as exceptional.
            write.write_all(&buf[0..bytes_read]).await?;
            copied += bytes_read;
        }

        if r2c && !(sv_tuple.4 == 0 && sv_tuple.5 == 0) {// only after UDP associate. either r2c or !r2c will be ok
            *udp_closed.lock().unwrap()=true;
            conmap.lock().await.remove(&sv_tuple);
            let mut stdin = stdin_writer.lock().await;
            stdin.write_all(&[sv_tuple.0, sv_tuple.1, sv_tuple.2, sv_tuple.3, 0, 0, sv_tuple.4, sv_tuple.5, 1]).await.unwrap();
            stdin.flush().await.unwrap();
        }
        Ok(copied)
    }

    loop {
        let (mut client, client_addr) = listener.accept().await?;
        let stdin_writer0 = Arc::clone(&stdin_writer);
        let conmap1 = Arc::clone(&conmap0);
        let local_addr = client.local_addr().unwrap().ip();
        tokio::spawn(async move {
            println!("New connection from {}", client_addr);

            // Establish connection to upstream for each incoming client connection
            let mut remote = match TcpStream::connect(remote).await {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("Error establishing upstream connection: {e}");
                    return;
                }
            };
            let (mut client_read, mut client_write) = client.split();
            let (mut remote_read, mut remote_write) = remote.split();

            let r2c = Arc::new(std::sync::Mutex::new(false));
            let (cancel, _) = broadcast::channel::<()>(1);
            let (remote_copied, client_copied) = tokio::join! {
                copy_with_abort(&mut remote_read, &mut client_write, cancel.subscribe(),true, Arc::clone(&r2c), Arc::clone(&stdin_writer0), Arc::clone(&conmap1), local_addr)
                    .then(|r| { let _ = cancel.send(()); async { r } }),
                copy_with_abort(&mut client_read, &mut remote_write, cancel.subscribe(),false, r2c            , Arc::clone(&stdin_writer0), Arc::clone(&conmap1), local_addr)
                    .then(|r| { let _ = cancel.send(()); async { r } }),
            };

            match client_copied {
                Ok(count) => {
                    if DEBUG.load(Ordering::Relaxed) {
                        eprintln!(
                            "Transferred {} bytes from proxy client {} to upstream server",
                            count, client_addr
                        );
                    }
                }
                Err(err) => {
                    eprintln!(
                        "Error writing bytes from proxy client {} to upstream server",
                        client_addr
                    );
                    eprintln!("{}", err);
                }
            };

            match remote_copied {
                Ok(count) => {
                    if DEBUG.load(Ordering::Relaxed) {
                        eprintln!(
                            "Transferred {} bytes from upstream server to proxy client {}",
                            count, client_addr
                        );
                    }
                }
                Err(err) => {
                    eprintln!(
                        "Error writing from upstream server to proxy client {}!",
                        client_addr
                    );
                    eprintln!("{}", err);
                }
            };

            ()
        });
    }
}
