use anyhow::{bail, Result};

pub const MAGIC_FPI1: &[u8; 4] = b"FPI1";
pub const HEADER_LEN: usize = 4 + 4 + 4 + 4 + 8;
pub const PAGE_SIZE: usize = 8192;
pub const RECORD_LEN: usize = HEADER_LEN + PAGE_SIZE;

#[derive(Debug, Clone)]
pub struct FpiRecord {
    pub rel_filenode: u32,
    pub fork: u32,
    pub block: u32,
    pub lsn: u64,
    pub page: Vec<u8>,
}

pub fn encode_fpi(r: &FpiRecord) -> Result<Vec<u8>> {
    if r.page.len() != PAGE_SIZE {
        bail!("page must be {PAGE_SIZE} bytes");
    }
    let mut buf = vec![0u8; RECORD_LEN];
    buf[0..4].copy_from_slice(MAGIC_FPI1);
    buf[4..8].copy_from_slice(&r.rel_filenode.to_be_bytes());
    buf[8..12].copy_from_slice(&r.fork.to_be_bytes());
    buf[12..16].copy_from_slice(&r.block.to_be_bytes());
    buf[16..24].copy_from_slice(&r.lsn.to_be_bytes());
    buf[24..].copy_from_slice(&r.page);
    Ok(buf)
}

pub fn decode_records(payload: &[u8]) -> Result<(Vec<FpiRecord>, Vec<u8>)> {
    let mut fps = Vec::new();
    let mut i = 0;
    while i + 4 <= payload.len() {
        if &payload[i..i + 4] != MAGIC_FPI1 {
            return Ok((fps, payload[i..].to_vec()));
        }
        if i + RECORD_LEN > payload.len() {
            bail!("truncated FPI record");
        }
        let mut page = vec![0u8; PAGE_SIZE];
        page.copy_from_slice(&payload[i + 24..i + RECORD_LEN]);
        fps.push(FpiRecord {
            rel_filenode: u32::from_be_bytes(payload[i + 4..i + 8].try_into()?),
            fork: u32::from_be_bytes(payload[i + 8..i + 12].try_into()?),
            block: u32::from_be_bytes(payload[i + 12..i + 16].try_into()?),
            lsn: u64::from_be_bytes(payload[i + 16..i + 24].try_into()?),
            page,
        });
        i += RECORD_LEN;
    }
    Ok((fps, Vec::new()))
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub tenant_id: String,
    pub timeline_id: String,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub payload: Vec<u8>,
}

pub fn encode_frame(f: &Frame) -> Vec<u8> {
    let tb = f.tenant_id.as_bytes();
    let tlb = f.timeline_id.as_bytes();
    let mut buf = Vec::with_capacity(2 + tb.len() + 2 + tlb.len() + 16 + f.payload.len());
    buf.extend_from_slice(&(tb.len() as u16).to_be_bytes());
    buf.extend_from_slice(tb);
    buf.extend_from_slice(&(tlb.len() as u16).to_be_bytes());
    buf.extend_from_slice(tlb);
    buf.extend_from_slice(&f.start_lsn.to_be_bytes());
    buf.extend_from_slice(&f.end_lsn.to_be_bytes());
    buf.extend_from_slice(&f.payload);
    buf
}

pub fn decode_frame(b: &[u8]) -> Result<Frame> {
    if b.len() < 4 + 16 {
        bail!("frame too short");
    }
    let mut o = 0;
    let tl = u16::from_be_bytes(b[o..o + 2].try_into()?) as usize;
    o += 2;
    if o + tl > b.len() {
        bail!("bad tenant len");
    }
    let tenant = String::from_utf8(b[o..o + tl].to_vec())?;
    o += tl;
    let tll = u16::from_be_bytes(b[o..o + 2].try_into()?) as usize;
    o += 2;
    if o + tll > b.len() {
        bail!("bad timeline len");
    }
    let timeline = String::from_utf8(b[o..o + tll].to_vec())?;
    o += tll;
    if o + 16 > b.len() {
        bail!("missing lsns");
    }
    let start = u64::from_be_bytes(b[o..o + 8].try_into()?);
    o += 8;
    let end = u64::from_be_bytes(b[o..o + 8].try_into()?);
    o += 8;
    Ok(Frame {
        tenant_id: tenant,
        timeline_id: timeline,
        start_lsn: start,
        end_lsn: end,
        payload: b[o..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fpi_roundtrip() {
        let page = vec![0xABu8; PAGE_SIZE];
        let enc = encode_fpi(&FpiRecord {
            rel_filenode: 16384,
            fork: 0,
            block: 1,
            lsn: 0x1000,
            page: page.clone(),
        })
        .unwrap();
        let (recs, unknown) = decode_records(&enc).unwrap();
        assert!(unknown.is_empty());
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].page, page);
        assert_eq!(recs[0].lsn, 0x1000);
    }

    #[test]
    fn frame_roundtrip() {
        let f = Frame {
            tenant_id: "t".into(),
            timeline_id: "tl".into(),
            start_lsn: 1,
            end_lsn: 2,
            payload: b"hi".to_vec(),
        };
        let got = decode_frame(&encode_frame(&f)).unwrap();
        assert_eq!(got.tenant_id, f.tenant_id);
        assert_eq!(got.payload, f.payload);
    }
}
