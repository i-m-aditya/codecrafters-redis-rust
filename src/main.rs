use anyhow::bail;
use anyhow::Result;
use async_recursion::async_recursion;
use bytes::Bytes;
use clap::Parser;
use hex_literal::hex;
use rand::distributions::{Alphanumeric, DistString};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
#[derive(Debug, Clone, PartialEq, Eq)]
// 6
enum RESP {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    RDB(Bytes),
    String(Bytes),
    Array(Vec<RESP>),
    Null,
}
async fn read_rdb_file<T>(r: &mut BufReader<T>) -> Result<RESP>
where
    T: AsyncReadExt + Unpin + Send,
{
    let mut buf = [0; 1];
    r.read(&mut buf).await?;
    match buf[0] {
        b'$' => {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            let len = line.trim().parse::<isize>()?;
            if len == -1 {
                return Ok(RESP::Null);
            }
            if len < 0 {
                return Err(anyhow::anyhow!("invalid bulk string length {len}"));
            }
            let mut data = vec![0; len as usize];
            r.read_exact(&mut data).await?;
            Ok(RESP::String(Bytes::from(data)))
        }
        t => Err(anyhow::anyhow!("invalid type {t:?}")),
    }
}
#[async_recursion]
async fn parse_redit_resp<T>(r: &mut BufReader<T>) -> Result<RESP>
where
    T: AsyncReadExt + Unpin + Send,
{
    let mut buf = [0; 1];
    r.read(&mut buf).await?;
    match buf[0] {
        b'+' => {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            // FIXME: trim is too aggressive, it should only remove the trailing \r\n
            Ok(RESP::String(Bytes::from(line.trim().to_string())))
        }
        b'-' => {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            // FIXME: trim is too aggressive, it should only remove the trailing \r\n
            Ok(RESP::Error(line.trim().to_string()))
        }
        b':' => {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            Ok(RESP::Integer(line.trim().parse::<i64>()?))
        }
        b'$' => {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            let len = line.trim().parse::<isize>()?;
            if len == -1 {
                return Ok(RESP::Null);
            }
            if len < 0 {
                return Err(anyhow::anyhow!("invalid bulk string length {len}"));
            }
            let mut data = vec![0; len as usize];
            r.read_exact(&mut data).await?;
            r.read_line(&mut line).await?;
            Ok(RESP::String(Bytes::from(data)))
        }
        b'*' => {
            let mut line = String::new();
            r.read_line(&mut line).await?;
            let len = line.trim().parse::<usize>()?;
            let mut array = Vec::with_capacity(len);
            for _ in 0..len {
                array.push(parse_redit_resp(r).await?);
            }
            Ok(RESP::Array(array))
        }
        t => Err(anyhow::anyhow!("Invalid type {t}")),
    }
}
#[async_recursion]
async fn write_resp<T: AsyncWriteExt + Unpin + Send>(resp: RESP, writer: &mut T) -> Result<()> {
    match resp {
        RESP::SimpleString(s) => {
            writer.write_all(format!("+{}\r\n", s).as_bytes()).await?;
        }
        RESP::Error(e) => {
            writer.write_all(format!("-{}\r\n", e).as_bytes()).await?;
        }
        RESP::Integer(i) => {
            writer.write_all(format!(":{}\r\n", i).as_bytes()).await?;
        }
        RESP::BulkString(b) => {
            writer
                .write_all(format!("${}\r\n", b.len()).as_bytes())
                .await?;
            writer.write_all(&b).await?;
            writer.write_all(b"\r\n").await?;
        }
        RESP::RDB(b) => {
            writer
                .write_all(format!("${}\r\n", b.len()).as_bytes())
                .await?;
            writer.write_all(&b).await?;
        }
        RESP::Array(a) => {
            writer
                .write_all(format!("*{}\r\n", a.len()).as_bytes())
                .await?;
            for r in a {
                write_resp(r, writer).await?;
            }
        }
        RESP::Null => {
            writer.write_all(b"$-1\r\n").await?;
        }
        RESP::String(b) => {
            writer
                .write_all(format!("${}\r\n", b.len()).as_bytes())
                .await?;
            writer.write_all(&b).await?;
            writer.write_all(b"\r\n").await?;
        }
    }
    Ok(())
}
async fn handle_client(mut stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<()> {
    let client_host = stream.local_addr()?.ip().to_string();
    let (reader, mut writer) = stream.split();
    let reader = &mut BufReader::new(reader);
    let (tx, mut rx) = channel::<RESP>(10);
    let client_id = {
        let mut db = db.lock().await;
        db.clients.push(Client {
            host: client_host.clone(),
            chan: tx,
        });
        db.clients.len() - 1
    };
    loop {
        tokio::select! {
            rec = parse_redit_resp(reader) => {
                match rec {
                    Ok(RESP::Array(vec)) => {
                        let [RESP::String(cmd), rest @ ..] = &vec[..] else {
                            println!("error invalid command {vec:?}");
                            continue;
                        };
                        let res = exec_cmd(client_id, String::from_utf8(cmd.to_vec())?.to_ascii_uppercase(), rest, &db, &mut writer).await;
                        if let Err(e) = res {
                            println!("error executing command {vec:?}: {e}");
                        }
                    }
                    Ok(v) => {
                        println!("error unknown command: {v:?}");
                        continue;
                    }
                    Err(e) => {
                        println!("error cannot parse command: {e}");
                        break;
                    }
                }
            }
            res = rx.recv() => {
                write_resp(res.unwrap(), &mut writer).await?;
            }
        }
    }
    Ok(())
}
async fn exec_cmd<T: AsyncWriteExt + Unpin + Send>(
    client_id: usize,
    cmd: String,
    rest: &[RESP],
    db: &Arc<Mutex<Db>>,
    mut writer: &mut T,
) -> Result<()> {
    // TODO most of the returns (with ?) should be continues (so put it in a function and return Result<()>)
    match cmd.as_str() {
        "PING" => {
            write_resp(RESP::SimpleString(String::from("PONG")), &mut writer).await?;
            println!("ping");
        }
        "ECHO" => {
            let [arg @ RESP::String(_)] = &rest[..] else {
                bail!("error invalid echo command {cmd:?} {rest:?}");
            };
            write_resp(arg.clone(), &mut writer).await?;
            println!("echo: {:?}", arg);
        }
        "GET" => {
            let [RESP::String(key)] = &rest[..] else {
                bail!("error invalid get command {cmd:?} {rest:?}");
            };
            let mut db = db.lock().await;
            let v = db.db.get(key);
            if let Some(v) = v {
                if let Some(expire) = v.expire {
                    println!("expire: {expire:?} now: {:?}", Instant::now());
                    if expire < Instant::now() {
                        println!("expired: {:?}", key);
                        db.db.remove(key);
                        write_resp(RESP::Null, &mut writer).await?;
                        return Ok(());
                    }
                    println!("not expired: {key:?}");
                }
                write_resp(RESP::BulkString(v.value.clone()), &mut writer).await?;
            } else {
                write_resp(RESP::Null, &mut writer).await?;
            }
            println!("get: {:?}", key);
        }
        "SET" => {
            let [RESP::String(key), RESP::String(value), rest @ ..] = &rest[..] else {
                bail!("error invalid set command {cmd:?} {rest:?}");
            };
            let mut expire = None;
            if rest.len() > 0 {
                let [RESP::String(cmd), RESP::String(arg)] = &rest[..] else {
                    bail!("error invalid set command {cmd:?} {rest:?}");
                };
                match String::from_utf8(cmd.to_vec())?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "PX" => {
                        let millis = String::from_utf8(arg.to_vec())?.parse::<u64>()?;
                        let now = Instant::now();
                        expire = Some(now + Duration::from_millis(millis));
                        println!("set expire: {:?} now: {:?}", expire, now);
                    }
                    _ => {
                        bail!("error invalid set command {cmd:?} {rest:?}");
                    }
                }
            }
            let mut db = db.lock().await;
            db.db.insert(
                key.clone(),
                Value {
                    value: value.clone(),
                    expire: expire,
                },
            );
            if let Role::Master { replicas, .. } = &db.role {
                for r in replicas {
                    let mut a = vec![
                        RESP::BulkString(Bytes::from("SET")),
                        RESP::BulkString(key.clone()),
                        RESP::BulkString(value.clone()),
                    ];
                    if let Some(expire) = expire {
                        a.push(RESP::BulkString(Bytes::from("PX")));
                        a.push(RESP::BulkString(Bytes::from(
                            (expire - Instant::now()).as_millis().to_string(),
                        )));
                    }
                    db.clients[r.client_id].chan.send(RESP::Array(a)).await?;
                }
            }
            println!("Trying to write response");
            write_resp(RESP::SimpleString(String::from("OK")), &mut writer).await?;
            println!("set: {:?} {:?}", key, value);
        }
        "INFO" => {
            let [RESP::String(section), _rest @ ..] = &rest[..] else {
                bail!("error invalid info command {cmd:?} {rest:?}");
            };
            assert!(
                section.to_ascii_uppercase() == b"REPLICATION",
                "invalid info section"
            );
            let db = db.lock().await;
            let mut info = String::new();
            info.push_str("# Replication\r\n");
            match &db.role {
                Role::Master { .. } => {
                    info.push_str("role:master\r\n");
                    info.push_str("master_replid:");
                    info.push_str(&db.id);
                    info.push_str("\r\n");
                    info.push_str("master_repl_offset:");
                    info.push_str(&db.offset.to_string());
                    info.push_str("\r\n");
                }
                Role::Slave { host, port, .. } => {
                    info.push_str("role:slave\r\n");
                    info.push_str("master_host:");
                    info.push_str(&host);
                    info.push_str("\r\n");
                    info.push_str("master_port:");
                    info.push_str(&port);
                    info.push_str("\r\n");
                }
            }
            info.push_str("role:master\r\n");
            write_resp(RESP::BulkString(Bytes::from(info)), &mut writer).await?;
            println!("info: {:?}", section);
        }
        "REPLCONF" => {
            let [RESP::String(cmd), RESP::String(arg), _rest @ ..] = &rest[..] else {
                bail!("error invalid replconf command {cmd:?} {rest:?}");
            };
            match String::from_utf8(cmd.to_vec())?
                .to_ascii_uppercase()
                .as_str()
            {
                "LISTENING-PORT" => {
                    let mut db = db.lock().await;
                    let Role::Master {
                        ref mut replicas, ..
                    } = &mut db.role
                    else {
                        bail!("error invalid replconf listening-port command {cmd:?} {rest:?} role: {:?}", db.role);
                    };
                    replicas.push(Replica {
                        client_id: client_id,
                        port: String::from_utf8(arg.to_vec())?,
                    });
                    write_resp(RESP::SimpleString(String::from("OK")), &mut writer).await?;
                }
                "CAPA" => {
                    write_resp(RESP::SimpleString(String::from("OK")), &mut writer).await?;
                }
                _ => {
                    bail!("error invalid replconf command {cmd:?} {rest:?}");
                }
            }
        }
        "PSYNC" => {
            let [RESP::String(id), RESP::String(offset)] = &rest[..] else {
                bail!("error invalid psync command {cmd:?} {rest:?}");
            };
            let db = db.lock().await;
            write_resp(
                RESP::SimpleString(format!("FULLRESYNC {} {}", db.id, db.offset)),
                &mut writer,
            )
            .await?;
            let empty_rdb = hex!("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
            writer
                .write_all(format!("${}\r\n", empty_rdb.len()).as_bytes())
                .await?;
            writer.write_all(&empty_rdb).await?;
            println!("psync: {:?} {:?}", id, offset);
        }
        cmd => {
            write_resp(RESP::SimpleString(String::from("OK")), &mut writer).await?;
            bail!("error unknown cmd: {cmd:?} args: {rest:?}");
        }
    }
    Ok(())
}
async fn get_resp<T>(r: &mut BufReader<T>) -> Result<String>
where
    T: AsyncReadExt + Unpin + Send,
{
    match parse_redit_resp(r).await {
        Ok(RESP::Array(vec)) => {
            let [RESP::String(cmd)] = &vec[..] else {
                println!("replication: expected pong, got {vec:?}");
                return Err(anyhow::anyhow!("invalid command"));
            };
            Ok(String::from_utf8(cmd.to_vec())?.to_ascii_uppercase())
        }
        Ok(RESP::String(cmd)) => Ok(String::from_utf8(cmd.to_vec())?.to_ascii_uppercase()),
        r => Err(anyhow::anyhow!("invalid command {r:?}")),
    }
}
async fn handle_replication(mut stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<()> {
    let (reader, mut writer) = stream.split();
    let reader = &mut BufReader::new(reader);
    write_resp(
        RESP::Array(vec![RESP::BulkString(Bytes::from("ping"))]),
        &mut writer,
    )
    .await
    .unwrap();
    assert_eq!(
        get_resp(reader).await?,
        "PONG",
        "replication: expected pong"
    );
    write_resp(
        RESP::Array(vec![
            RESP::BulkString(Bytes::from("replconf")),
            RESP::BulkString(Bytes::from("listening-port")),
            RESP::BulkString(Bytes::from(db.lock().await.port.clone())),
        ]),
        &mut writer,
    )
    .await
    .unwrap();
    assert_eq!(get_resp(reader).await?, "OK", "replication: expected ok");
    write_resp(
        RESP::Array(vec![
            RESP::BulkString(Bytes::from("replconf")),
            RESP::BulkString(Bytes::from("capa")),
            RESP::BulkString(Bytes::from("psync2")),
        ]),
        &mut writer,
    )
    .await
    .unwrap();
    assert_eq!(get_resp(reader).await?, "OK", "replication: expected ok");
    write_resp(
        RESP::Array(vec![
            RESP::BulkString(Bytes::from("psync")),
            RESP::BulkString(Bytes::from("?")),
            RESP::BulkString(Bytes::from("-1")),
        ]),
        &mut writer,
    )
    .await
    .unwrap();
    let _res = get_resp(reader).await?;
    let _res = parse_redit_resp(reader).await?;

    let _res = read_rdb_file(reader).await?;
    println!("replication: rdb file read");
    loop {
        let rec = parse_redit_resp(reader).await;
        match rec {
            Ok(RESP::Array(vec)) => {
                let [RESP::String(cmd), rest @ ..] = &vec[..] else {
                    println!("error invalid command {vec:?}");
                    continue;
                };
                let res = exec_cmd(
                    99999999,
                    String::from_utf8(cmd.to_vec())?.to_ascii_uppercase(),
                    rest,
                    &db,
                    &mut tokio::io::sink(),
                )
                .await;
                if let Err(e) = res {
                    println!("error executing command {vec:?}: {e}");
                }
            }
            Ok(v) => {
                println!("error unknown command: {v:?}");
                continue;
            }
            Err(e) => {
                println!("error cannot parse command: {e}");
                return Err(e);
            }
        }
    }
}
#[derive(Debug, Clone)]
struct Value {
    value: Bytes,
    expire: Option<Instant>,
}
#[derive(Debug, Clone)]
struct Replica {
    client_id: usize,
    port: String,
}
#[derive(Debug, Clone)]
enum Role {
    Master { replicas: Vec<Replica> },
    Slave { host: String, port: String },
}
#[derive(Debug, Clone)]
struct Client {
    host: String,
    chan: Sender<RESP>,
}
#[derive(Debug, Clone)]
struct Db {
    db: HashMap<Bytes, Value>,
    clients: Vec<Client>,
    role: Role,
    id: String,
    offset: u64,
    port: String,
}
#[derive(Parser, Debug)]
#[command()]
struct Args {
    #[arg(long, default_value = "6379")]
    port: String,
    #[arg(long)]
    replicaof: bool,
    replicaof_host: Option<String>,
    replicaof_port: Option<String>,
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    assert!(
        args.replicaof_host.is_none() == args.replicaof_port.is_none(),
        "replicaof_host and replicaof_port must be both set or both unset"
    );
    assert!(
        args.replicaof_host.is_none() == !args.replicaof,
        "replicaof_host and replicaof must be both set or both unset"
    );
    let listener = TcpListener::bind("127.0.0.1:".to_string() + &args.port)
        .await
        .unwrap();
    println!("listening on 127.0.0.1:6379");
    // redis ID: 40 random alphanumeric characters
    // FIXME: This makes letters twice as likely as numbers because uppercase letters are
    // lowercased
    let id = Alphanumeric
        .sample_string(&mut rand::thread_rng(), 40)
        .to_ascii_lowercase();
    let role = if let (Some(host), Some(port)) = (args.replicaof_host, args.replicaof_port) {
        Role::Slave { host, port }
    } else {
        Role::Master { replicas: vec![] }
    };
    let db = Arc::new(Mutex::new(Db {
        db: HashMap::new(),
        role: role.clone(),
        id: id,
        offset: 0,
        port: args.port,
        clients: vec![],
    }));
    if let Role::Slave { host, port, .. } = role {
        let socket = TcpStream::connect(String::new() + &host + ":" + &port)
            .await
            .unwrap();
        let db = db.clone();
        tokio::spawn(async move {
            handle_replication(socket, db).await.unwrap();
        });
    }
    loop {
        let (stream, ip) = listener.accept().await.unwrap();
        println!("accepted client {ip}");
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, db).await {
                println!("error handling client {ip}: {e}");
            }
        });
    }
}
