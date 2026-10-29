use std::io::{self, Read, Write};

use flate2::{read::GzDecoder, Compression, GzBuilder};

pub fn compress(body: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    encoder.write_all(body)?;
    encoder.finish()
}

pub fn decompress(body: &[u8], limit: usize) -> io::Result<String> {
    let mut decoded = Vec::new();
    GzDecoder::new(body)
        .take(limit as u64 + 1)
        .read_to_end(&mut decoded)?;
    if decoded.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "report body exceeds limit",
        ));
    }
    String::from_utf8(decoded).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
