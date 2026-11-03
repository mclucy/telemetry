use std::io::{self, Write};

use flate2::{Compression, GzBuilder};

pub fn compress(body: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    encoder.write_all(body)?;
    encoder.finish()
}
