//! Minimal systemd notification support without a libsystemd dependency.

use std::io;

pub fn notify(message: &str) -> io::Result<()> {
    let Some(socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        send_unix_datagram(socket.as_encoded_bytes(), message.as_bytes())
    }
    #[cfg(not(unix))]
    {
        let _ = (socket, message);
        Ok(())
    }
}

pub fn watchdog_interval() -> Option<std::time::Duration> {
    let micros = std::env::var("WATCHDOG_USEC").ok()?.parse::<u64>().ok()?;
    (micros > 0).then(|| std::time::Duration::from_micros((micros / 2).max(1)))
}

#[cfg(unix)]
fn send_unix_datagram(path: &[u8], message: &[u8]) -> io::Result<()> {
    use std::mem::{size_of, zeroed};

    if path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty NOTIFY_SOCKET",
        ));
    }
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        let mut address: libc::sockaddr_un = unsafe { zeroed() };
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let abstract_socket = path[0] == b'@';
        let name = if abstract_socket { &path[1..] } else { path };
        let required = name.len() + usize::from(!abstract_socket);
        if required > address.sun_path.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NOTIFY_SOCKET path is too long",
            ));
        }
        let start = usize::from(abstract_socket);
        for (destination, source) in address.sun_path[start..].iter_mut().zip(name) {
            *destination = *source as libc::c_char;
        }
        let path_offset = size_of::<libc::sa_family_t>();
        let address_length = path_offset + start + name.len() + usize::from(!abstract_socket);
        let sent = unsafe {
            libc::sendto(
                fd,
                message.as_ptr().cast(),
                message.len(),
                libc::MSG_NOSIGNAL,
                (&address as *const libc::sockaddr_un).cast(),
                address_length as libc::socklen_t,
            )
        };
        if sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })();
    unsafe { libc::close(fd) };
    result
}
