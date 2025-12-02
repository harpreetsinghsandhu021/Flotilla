use ssh2;
use std::io::{ErrorKind, Result};
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

pub struct Session {
    ssh: ssh2::Session,
    // stream: TcpStream,
}

impl Session {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let private_key_path = Path::new("/Users/harpreetsingh/Downloads/flotilla-key-pair.pem");
        let public_key_path = Path::new("/Users/harpreetsingh/Downloads/flotilla-key-pair.pub");
        let start = Instant::now();
        let timeout = Duration::from_secs(120);

        let tcp = loop {
            // 1. Try to connect
            match TcpStream::connect(&addr) {
                Ok(stream) => {
                    println!("SSH port is open!");
                    break stream;
                }
                Err(e) => {
                    // 2. If timeout reached, crash with the error
                    if start.elapsed() > timeout {
                        // panic!("Timed out waiting for SSH on {}: {}", addr, e);
                    }

                    // 3. If Connection Refused, wait and retry
                    // We also handle "Resource temporarily unavailable" which can happen on bad networks
                    match e.kind() {
                        ErrorKind::ConnectionRefused | ErrorKind::TimedOut => {
                            print!("."); // distinct visual feedback
                            std::io::Write::flush(&mut std::io::stdout()).unwrap();
                            thread::sleep(Duration::from_secs(1));
                        }
                        _ => {}
                    }
                }
            }
        };

        let mut sess = ssh2::Session::new()?;
        sess.set_tcp_stream(tcp);
        sess.handshake()?;

        sess.userauth_pubkey_file(
            "ec2-user",
            Some(public_key_path), // Optional public key path
            private_key_path,
            Some("flotilla"), // Passphrase (use Some("passphrase") if encrypted)
        )?;

        Ok(Session {
            ssh: sess,
            // stream: null,
        })
    }

    pub fn cmd(&mut self, cmd: &str) -> Result<String> {
        use std::io::Read;

        let mut channel = self.ssh.channel_session()?;
        channel.exec(cmd)?;
        let mut s = String::new();
        channel.read_to_string(&mut s)?;
        channel.wait_close()?;
        // println!("{}", channel.exit_status()?);
        Ok(s)
    }
}

impl Deref for Session {
    type Target = ssh2::Session;
    fn deref(&self) -> &Self::Target {
        &self.ssh
    }
}

impl DerefMut for Session {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ssh
    }
}
