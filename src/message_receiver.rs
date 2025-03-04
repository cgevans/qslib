use regex::bytes::Regex;
use std::sync::LazyLock;
use thiserror::Error;

#[cfg(feature = "simd")]
use memchr::memchr3;

static TAG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^<(/?)([A-Za-z0-9_.-]*)(>|$)").unwrap());

/*
This isn't quite right: the quote mismatchees for InstrumentServer appear to be at a line level.
*/

#[derive(Error, Debug)]
pub enum MsgReceiveError {
    #[error("Unexpected close tag: </{1}> at {0}.")]
    UnexpectedCloseTag(usize, String),
    #[error("Mismatched close tag: </{2}> at {1} closes <{3}> at {0}.")]
    MismatchedCloseTag(usize, usize, String, String),
}

#[derive(Error, Debug)]
pub enum MsgPushError {
    #[error("Message waiting.")]
    MessageWaiting,
}

pub struct MsgRecv {
    buf: Vec<u8>,
    tagstack: Vec<(Vec<u8>, usize)>,
    parttag: Option<usize>,
    msg_end: Option<usize>,
    msg_error: Option<MsgReceiveError>,
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl std::fmt::Debug for MsgRecv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MsgRecv {{ buf: {:?}, tagstack: {:?}, parttag: {:?}, msg_end: {:?}, msg_error: {:?} }}", String::from_utf8_lossy(&self.buf), self.tagstack.iter().map(|(tag, _)| String::from_utf8_lossy(tag)).collect::<Vec<_>>(), self.parttag, self.msg_end, self.msg_error)
    }
}

impl Default for MsgRecv {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgRecv {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1024),
            tagstack: Vec::new(),
            parttag: None,
            msg_end: None,
            msg_error: None,
        }
    }

    pub fn try_get_msg(&mut self) -> Result<Option<Vec<u8>>, MsgReceiveError> {
        match self.msg_end {
            Some(msg_end) => {
                let msg = Vec::from_iter(self.buf.drain(0..msg_end));
                self.msg_end = None;
                let cur_error = self.msg_error.take();
                self.tagstack.clear();
                self.parttag = None;
                let _another = self.check_from_pos(0).unwrap(); // We know no message is waiting now.
                if let Some(err) = cur_error {
                    return Err(err);
                }
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    fn check_from_pos(&mut self, start_pos: usize) -> Result<bool, MsgPushError> {
        if self.msg_error.is_some() || self.msg_end.is_some() {
            return Ok(true);
        }
        self.parttag = None;
        let mut pos = start_pos;

        #[cfg(feature = "simd")]
        while let Some(offset) = memchr3(b'<', b'\n', b'>', &self.buf[pos..]) {
            let c = self.buf[pos + offset];
            if c == b'\n' {
                if self.tagstack.len() == 0 {
                    self.msg_end = Some(pos + offset + 1);
                    return Ok(true);
                }
            } else if c == b'<' {
                match TAG_REGEX.captures(&self.buf[pos + offset..]) {
                    Some(captures) => {
                        let (_a, [close, tag, end]) = captures.extract();
                        match (end, close) {
                            (b"", _) => {
                                self.parttag = Some(pos + offset);
                                return Ok(false);
                            }
                            (_, b"/") => match self.tagstack.pop() {
                                Some(old_tag) => {
                                    if old_tag.0 != tag {
                                        self.msg_error = Some(MsgReceiveError::MismatchedCloseTag(
                                            old_tag.1,
                                            pos + offset,
                                            String::from_utf8_lossy(&old_tag.0).to_string(),
                                            String::from_utf8_lossy(&tag).to_string(),
                                        ));
                                        self.tagstack.clear();
                                    }
                                }
                                None => {
                                    self.msg_error = Some(MsgReceiveError::UnexpectedCloseTag(
                                        pos + offset,
                                        String::from_utf8_lossy(&tag).to_string(),
                                    ));
                                    self.tagstack.clear();
                                }
                            },
                            (_, _) => {
                                // if self.msg_error.is_none() {
                                self.tagstack.push((tag.to_vec(), pos + offset));
                                // }
                            }
                        }
                    }
                    None => {}
                }
            }
            pos += offset + 1;
        }

        #[cfg(not(feature = "simd"))]
        while let Some(offset) = self.buf[pos..]
            .iter()
            .position(|&c| c == b'<' || c == b'\n' || c == b'>')
        {
            let c = self.buf[pos + offset];
            if c == b'\n' {
                if self.tagstack.is_empty() {
                    self.msg_end = Some(pos + offset + 1);
                    return Ok(true);
                }
            } else if c == b'<' {
                if let Some(captures) = TAG_REGEX.captures(&self.buf[pos + offset..]) {
                    let (_a, [close, tag, end]) = captures.extract();
                    match (end, close) {
                        (b"", _) => {
                            self.parttag = Some(pos + offset);
                            return Ok(false);
                        }
                        (_, b"/") => match self.tagstack.pop() {
                            Some(old_tag) => {
                                if old_tag.0 != tag {
                                    self.msg_error = Some(MsgReceiveError::MismatchedCloseTag(
                                        old_tag.1,
                                        pos + offset,
                                        String::from_utf8_lossy(&old_tag.0).to_string(),
                                        String::from_utf8_lossy(tag).to_string(),
                                    ));
                                    self.tagstack.clear();
                                }
                            }
                            None => {
                                self.msg_error = Some(MsgReceiveError::UnexpectedCloseTag(
                                    pos + offset,
                                    String::from_utf8_lossy(tag).to_string(),
                                ));
                                self.tagstack.clear();
                            }
                        },
                        (_, _) => {
                            // if self.msg_error.is_none() {
                            self.tagstack.push((tag.to_vec(), pos + offset));
                            // }
                        }
                    }
                }
            }
            pos += offset + 1;
        }

        Ok(false)
    }

    pub fn push_data(&mut self, data: &[u8]) -> Result<bool, MsgPushError> {
        let last_pos = self.parttag.unwrap_or(self.buf.len());
        self.buf.extend_from_slice(data);
        self.check_from_pos(last_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_while_message_waiting() {
        let mut receiver = MsgRecv::new();

        // Push first complete message
        receiver.push_data(b"OK 1 success\n").unwrap();

        // Push second message while first is still waiting
        receiver.push_data(b"OK 2 also success\n").unwrap();

        // Now retrieve both messages
        let msg1 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg1, b"OK 1 success\n");

        let msg2 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg2, b"OK 2 also success\n");

        // Verify no more messages
        assert!(receiver.try_get_msg().unwrap().is_none());
    }

    #[test]
    fn test_push_partial_while_message_waiting() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"OK 1 success\n").unwrap();
        receiver.push_data(b"OK 2 ").unwrap();
        receiver.push_data(b"also success\n").unwrap();
        let msg1 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg1, b"OK 1 success\n");
        let msg2 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg2, b"OK 2 also success\n");
    }

    #[test]
    fn test_push_partial_xml_tag() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"<test").unwrap();
        receiver.push_data(b">OK 2 \n").unwrap();
        receiver
            .push_data(b"also <another>\n\n\r\n</another> success</te")
            .unwrap();

        // Push the closing tag
        receiver.push_data(b"st>\n").unwrap();

        let msg = receiver.try_get_msg();

        assert_eq!(
            String::from_utf8_lossy(&msg.unwrap().unwrap()),
            "<test>OK 2 \nalso <another>\n\n\r\n</another> success</test>\n"
        );
    }
}
