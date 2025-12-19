use regex::bytes::Regex;
use std::sync::LazyLock;
use thiserror::Error;

#[cfg(feature = "simd")]
use memchr::memchr3;

static TAG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^<(/?)([A-Za-z0-9_.-]*)(>|$)").expect("Invalid TAG_REGEX pattern"));

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
                let _another = self.check_from_pos(0); // We know no message is waiting now.
                if let Some(err) = cur_error {
                    return Err(err);
                }
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    fn check_from_pos(&mut self, start_pos: usize) -> bool {
        if self.msg_error.is_some() || self.msg_end.is_some() {
            return true;
        }
        self.parttag = None;
        let mut pos = start_pos;

        #[cfg(feature = "simd")]
        while let Some(offset) = memchr3(b'<', b'\n', b'>', &self.buf[pos..]) {
            // Defensive check: ensure we don't index out of bounds
            let idx = pos + offset;
            if idx >= self.buf.len() {
                break;
            }
            let c = self.buf[idx];
            if c == b'\n' {
                if self.tagstack.len() == 0 {
                    self.msg_end = Some(idx + 1);
                    return true;
                }
            } else if c == b'<' {
                match TAG_REGEX.captures(&self.buf[idx..]) {
                    Some(captures) => {
                        let (_a, [close, tag, end]) = captures.extract();
                        match (end, close) {
                            (b"", _) => {
                                self.parttag = Some(idx);
                                return false;
                            }
                            (_, b"/") => match self.tagstack.pop() {
                                Some(old_tag) => {
                                    if old_tag.0 != tag {
                                        self.msg_error = Some(MsgReceiveError::MismatchedCloseTag(
                                            old_tag.1,
                                            idx,
                                            String::from_utf8_lossy(&old_tag.0).to_string(),
                                            String::from_utf8_lossy(&tag).to_string(),
                                        ));
                                        self.tagstack.clear();
                                    }
                                }
                                None => {
                                    self.msg_error = Some(MsgReceiveError::UnexpectedCloseTag(
                                        idx,
                                        String::from_utf8_lossy(&tag).to_string(),
                                    ));
                                    self.tagstack.clear();
                                }
                            },
                            (_, _) => {
                                // if self.msg_error.is_none() {
                                self.tagstack.push((tag.to_vec(), idx));
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
            // Defensive check: ensure we don't index out of bounds
            let idx = pos + offset;
            if idx >= self.buf.len() {
                break;
            }
            let c = self.buf[idx];
            if c == b'\n' {
                if self.tagstack.is_empty() {
                    self.msg_end = Some(idx + 1);
                    return true;
                }
            } else if c == b'<' {
                if let Some(captures) = TAG_REGEX.captures(&self.buf[idx..]) {
                    let (_a, [close, tag, end]) = captures.extract();
                    match (end, close) {
                        (b"", _) => {
                            self.parttag = Some(idx);
                            return false;
                        }
                        (_, b"/") => match self.tagstack.pop() {
                            Some(old_tag) => {
                                if old_tag.0 != tag {
                                    self.msg_error = Some(MsgReceiveError::MismatchedCloseTag(
                                        old_tag.1,
                                        idx,
                                        String::from_utf8_lossy(&old_tag.0).to_string(),
                                        String::from_utf8_lossy(tag).to_string(),
                                    ));
                                    self.tagstack.clear();
                                }
                            }
                            None => {
                                self.msg_error = Some(MsgReceiveError::UnexpectedCloseTag(
                                    idx,
                                    String::from_utf8_lossy(tag).to_string(),
                                ));
                                self.tagstack.clear();
                            }
                        },
                        (_, _) => {
                            // if self.msg_error.is_none() {
                            self.tagstack.push((tag.to_vec(), idx));
                            // }
                        }
                    }
                }
            }
            pos += offset + 1;
        }

        false
    }

    /// Push data into the message receiver buffer.
    ///
    /// This function appends the provided data to the internal buffer and checks
    /// if a complete message is available. A complete message is determined by
    /// finding a newline character when the XML tag stack is empty.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte slice to append to the buffer
    ///
    /// # Returns
    ///
    /// Returns `true` if a complete message is ready to be retrieved (via `try_get_msg`),
    /// or if there's a message error. Returns `false` if more data is needed or if
    /// parsing encountered a partial XML-like tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use qslib::message_receiver::MsgRecv;
    ///
    /// let mut receiver = MsgRecv::new();
    /// let ready = receiver.push_data(b"OK 1 success\n");
    /// assert!(ready);
    /// ```
    pub fn push_data(&mut self, data: &[u8]) -> bool {
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
        receiver.push_data(b"OK 1 success\n");

        // Push second message while first is still waiting
        receiver.push_data(b"OK 2 also success\n");

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
        receiver.push_data(b"OK 1 success\n");
        receiver.push_data(b"OK 2 ");
        receiver.push_data(b"also success\n");
        let msg1 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg1, b"OK 1 success\n");
        let msg2 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg2, b"OK 2 also success\n");
    }

    #[test]
    fn test_push_partial_xml_tag() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"<test");
        receiver.push_data(b">OK 2 \n");
        receiver.push_data(b"also <another>\n\n\r\n</another> success</te");

        // Push the closing tag
        receiver.push_data(b"st>\n");

        let msg = receiver.try_get_msg();

        assert_eq!(
            String::from_utf8_lossy(&msg.unwrap().unwrap()),
            "<test>OK 2 \nalso <another>\n\n\r\n</another> success</test>\n"
        );
    }

    // =====================================================================
    // Additional message receiver tests
    // =====================================================================

    #[test]
    fn test_simple_message() {
        let mut receiver = MsgRecv::new();
        let ready = receiver.push_data(b"OK 1 success\n");
        assert!(ready, "Should signal message ready");
        
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg, b"OK 1 success\n");
    }

    #[test]
    fn test_no_message_without_newline() {
        let mut receiver = MsgRecv::new();
        let ready = receiver.push_data(b"OK 1 success");
        assert!(!ready, "Should not signal ready without newline");
        
        let msg = receiver.try_get_msg().unwrap();
        assert!(msg.is_none(), "Should not have a message yet");
    }

    #[test]
    fn test_empty_message() {
        let mut receiver = MsgRecv::new();
        let ready = receiver.push_data(b"\n");
        assert!(ready, "Empty line is still a complete message");
        
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg, b"\n");
    }

    #[test]
    fn test_xml_preserves_internal_newlines() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"OK 1 <quote>line1\nline2\nline3</quote>\n");
        
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert!(String::from_utf8_lossy(&msg).contains("line1\nline2\nline3"));
    }

    #[test]
    fn test_nested_xml_tags() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"OK 1 <outer><inner>content\nwith\nnewlines</inner></outer>\n");
        
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert!(String::from_utf8_lossy(&msg).contains("<outer><inner>content\nwith\nnewlines</inner></outer>"));
    }

    #[test]
    fn test_mismatched_close_tag_error() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"<tag1>content</tag2>\n");
        
        let result = receiver.try_get_msg();
        assert!(result.is_err(), "Mismatched tags should produce an error");
    }

    #[test]
    fn test_unexpected_close_tag_error() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"</unexpected>content\n");
        
        let result = receiver.try_get_msg();
        assert!(result.is_err(), "Unexpected close tag should produce an error");
    }

    #[test]
    fn test_multiple_messages_in_one_push() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"msg1\nmsg2\nmsg3\n");
        
        let msg1 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg1, b"msg1\n");
        
        let msg2 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg2, b"msg2\n");
        
        let msg3 = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg3, b"msg3\n");
        
        assert!(receiver.try_get_msg().unwrap().is_none());
    }

    #[test]
    fn test_very_long_xml_content() {
        let mut receiver = MsgRecv::new();
        let long_content = "x".repeat(10000);
        let input = format!("OK 1 <data>{}</data>\n", long_content);
        receiver.push_data(input.as_bytes());
        
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert!(msg.len() > 10000);
    }

    #[test]
    fn test_receiver_reset_after_error() {
        let mut receiver = MsgRecv::new();
        
        // First, cause an error
        receiver.push_data(b"</unexpected>error\n");
        let _ = receiver.try_get_msg(); // Consume the error
        
        // Should work normally after
        receiver.push_data(b"OK 1 success\n");
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert_eq!(&msg, b"OK 1 success\n");
    }

    #[test]
    fn test_special_xml_tag_names() {
        let mut receiver = MsgRecv::new();
        receiver.push_data(b"OK 1 <multiline.protocol>content</multiline.protocol>\n");
        
        let msg = receiver.try_get_msg().unwrap().unwrap();
        assert!(String::from_utf8_lossy(&msg).contains("multiline.protocol"));
    }

    #[test]
    fn test_default_impl() {
        let mut receiver: MsgRecv = Default::default();
        assert!(receiver.try_get_msg().is_ok());
    }

    #[test]
    fn test_angle_brackets_in_content_not_tags() {
        // This tests the case where angle brackets appear in content but are NOT
        // valid XML-style tags (e.g., from `ip addr` output like <LOOPBACK,UP,LOWER_UP>)
        let mut receiver = MsgRecv::new();
        let ip_addr_output = b"OK 123 <quote.reply>1: lo: <LOOPBACK,UP,LOWER_UP> mtu 16436\n2: eth0: <BROADCAST,MULTICAST,UP> mtu 1500\n</quote.reply>\n";
        receiver.push_data(ip_addr_output);

        let msg = receiver.try_get_msg().unwrap().unwrap();
        let msg_str = String::from_utf8_lossy(&msg);
        assert!(msg_str.contains("<LOOPBACK,UP,LOWER_UP>"), "Should preserve angle brackets in content");
        assert!(msg_str.contains("<BROADCAST,MULTICAST,UP>"), "Should preserve angle brackets in content");
    }
}
