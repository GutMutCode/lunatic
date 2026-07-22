//! Language-neutral wire contract for guest-Wasm OTP adapters.
//!
//! Guest SDKs build these envelopes with the existing `lunatic::message`
//! imports. Calls use `send_receive_skip_search`, casts use `send`, replies are
//! tagged with `reply_tag`, and a stop request is acknowledged before the
//! server process exits. The runtime therefore retains its bounded mailbox and
//! timeout behavior without adding language-specific host imports.

use anyhow::{anyhow, bail, Result};

pub const GUEST_OTP_MAGIC: u32 = u32::from_le_bytes(*b"OTP1");
pub const GUEST_OTP_HEADER_LEN: usize = 24;

/// Existing `lunatic::message` status returned for a queued send or reply.
pub const GUEST_OTP_OK: u32 = 0;
/// Existing status for a closed or missing target process.
pub const GUEST_OTP_TARGET_MISSING: u32 = 1;
/// Existing status for destination backpressure or size rejection.
pub const GUEST_OTP_BACKPRESSURE: u32 = 2;
/// Existing `lunatic::message` timeout status.
pub const GUEST_OTP_TIMEOUT: u32 = 9_027;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum GuestOtpKind {
    Call = 1,
    Cast = 2,
    Stop = 3,
    Reply = 4,
}

impl TryFrom<u32> for GuestOtpKind {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            1 => Ok(Self::Call),
            2 => Ok(Self::Cast),
            3 => Ok(Self::Stop),
            4 => Ok(Self::Reply),
            _ => bail!("unknown guest OTP message kind {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestOtpHeader {
    pub kind: GuestOtpKind,
    /// Guest process that should receive the reply. Casts use zero.
    pub caller_process_id: u64,
    /// Selective-receive tag used to correlate a call or stop acknowledgement.
    pub reply_tag: i64,
}

impl GuestOtpHeader {
    pub fn call(caller_process_id: u64, reply_tag: i64) -> Result<Self> {
        validate_reply_target(caller_process_id, reply_tag)?;
        Ok(Self {
            kind: GuestOtpKind::Call,
            caller_process_id,
            reply_tag,
        })
    }

    pub fn cast() -> Self {
        Self {
            kind: GuestOtpKind::Cast,
            caller_process_id: 0,
            reply_tag: 0,
        }
    }

    pub fn stop(caller_process_id: u64, reply_tag: i64) -> Result<Self> {
        validate_reply_target(caller_process_id, reply_tag)?;
        Ok(Self {
            kind: GuestOtpKind::Stop,
            caller_process_id,
            reply_tag,
        })
    }

    pub fn reply(reply_tag: i64) -> Result<Self> {
        if reply_tag == 0 {
            bail!("guest OTP replies require a non-zero correlation tag");
        }
        Ok(Self {
            kind: GuestOtpKind::Reply,
            caller_process_id: 0,
            reply_tag,
        })
    }

    pub fn encode(self) -> [u8; GUEST_OTP_HEADER_LEN] {
        let mut bytes = [0; GUEST_OTP_HEADER_LEN];
        bytes[0..4].copy_from_slice(&GUEST_OTP_MAGIC.to_le_bytes());
        bytes[4..8].copy_from_slice(&(self.kind as u32).to_le_bytes());
        bytes[8..16].copy_from_slice(&self.caller_process_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.reply_tag.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < GUEST_OTP_HEADER_LEN {
            bail!("guest OTP envelope is shorter than {GUEST_OTP_HEADER_LEN} bytes");
        }
        let magic = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| anyhow!("invalid guest OTP magic field"))?,
        );
        if magic != GUEST_OTP_MAGIC {
            bail!("guest OTP envelope has an invalid magic value");
        }
        let kind = GuestOtpKind::try_from(u32::from_le_bytes(
            bytes[4..8]
                .try_into()
                .map_err(|_| anyhow!("invalid guest OTP kind field"))?,
        ))?;
        let caller_process_id = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| anyhow!("invalid guest OTP caller field"))?,
        );
        let reply_tag = i64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| anyhow!("invalid guest OTP reply-tag field"))?,
        );

        match kind {
            GuestOtpKind::Call | GuestOtpKind::Stop => {
                validate_reply_target(caller_process_id, reply_tag)?
            }
            GuestOtpKind::Cast if caller_process_id != 0 || reply_tag != 0 => {
                bail!("guest OTP cast must not contain a reply target")
            }
            GuestOtpKind::Reply if caller_process_id != 0 || reply_tag == 0 => {
                bail!("guest OTP reply requires an empty caller and a non-zero correlation tag")
            }
            GuestOtpKind::Cast | GuestOtpKind::Reply => {}
        }

        Ok(Self {
            kind,
            caller_process_id,
            reply_tag,
        })
    }
}

pub fn encode_guest_otp_message(header: GuestOtpHeader, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(GUEST_OTP_HEADER_LEN + payload.len());
    message.extend_from_slice(&header.encode());
    message.extend_from_slice(payload);
    message
}

pub fn decode_guest_otp_message(bytes: &[u8]) -> Result<(GuestOtpHeader, &[u8])> {
    let header = GuestOtpHeader::decode(bytes)?;
    Ok((header, &bytes[GUEST_OTP_HEADER_LEN..]))
}

fn validate_reply_target(caller_process_id: u64, reply_tag: i64) -> Result<()> {
    if caller_process_id == 0 {
        bail!("guest OTP request requires a non-zero caller process ID");
    }
    if reply_tag == 0 {
        bail!("guest OTP request requires a non-zero correlation tag");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_header_round_trips_without_native_layout_assumptions() {
        let header = GuestOtpHeader::call(42, -77).unwrap();
        let message = encode_guest_otp_message(header, b"payload");
        let (decoded, payload) = decode_guest_otp_message(&message).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(payload, b"payload");

        assert_eq!(
            GuestOtpHeader::reply(0x0102_0304_0506_0708)
                .unwrap()
                .encode(),
            [b'O', b'T', b'P', b'1', 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1,]
        );
    }

    #[test]
    fn malformed_or_uncorrelated_requests_fail_closed() {
        assert!(GuestOtpHeader::call(0, 1).is_err());
        assert!(GuestOtpHeader::call(1, 0).is_err());
        assert!(GuestOtpHeader::decode(b"short").is_err());

        let mut cast = GuestOtpHeader::cast().encode();
        cast[8..16].copy_from_slice(&1_u64.to_le_bytes());
        assert!(GuestOtpHeader::decode(&cast).is_err());

        let mut reply = GuestOtpHeader::reply(1).unwrap().encode();
        reply[8..16].copy_from_slice(&1_u64.to_le_bytes());
        assert!(GuestOtpHeader::decode(&reply).is_err());
    }
}
