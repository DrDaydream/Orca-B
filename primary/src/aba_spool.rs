//! Overflow of the bounded ABA broadcast channel. A paused peer cannot
//! hold the command router while consensus has commit feedback to deliver.
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, Write};

type Batch = Vec<Vec<u8>>;

fn temporary_file() -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_DELETE_ON_CLOSE: u32 = 0x04000000;
        options.custom_flags(FILE_FLAG_DELETE_ON_CLOSE);
    }
    loop {
        let path = format!(".orca-aba-{:032x}.tmp", rand::random::<u128>());
        match options.open(&path) {
            Ok(file) => {
                #[cfg(unix)]
                std::fs::remove_file(path)?;
                return Ok(file);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

#[derive(Default)]
pub(crate) struct AbaSpool {
    // Two anonymous files bound resident memory and release consumed disk
    // space even when ingress continues throughout a lengthy catch-up.
    reading: Option<(BufReader<File>, usize)>,
    writing: Option<(BufWriter<File>, usize)>,
}

impl AbaSpool {
    pub(crate) fn is_empty(&self) -> bool {
        self.reading.is_none() && self.writing.is_none()
    }

    pub(crate) async fn push(&mut self, batch: Batch) {
        let writing = self.writing.take();
        self.writing = Some(
            tokio::task::spawn_blocking(move || {
                let (mut writer, count) = writing.unwrap_or_else(|| {
                    (
                        BufWriter::new(
                            temporary_file().expect("Failed to open temporary ABA outbox"),
                        ),
                        0,
                    )
                });
                bincode::serialize_into(&mut writer, &batch).expect("Failed to spool ABA batch");
                (writer, count + 1)
            })
            .await
            .expect("ABA spool writer stopped"),
        );
    }

    pub(crate) async fn pop(&mut self) -> Batch {
        let reading = self.reading.take();
        let writing = if reading.is_none() {
            self.writing.take()
        } else {
            None
        };
        let (remaining, batch) = tokio::task::spawn_blocking(move || {
            let (mut reader, count) = reading.unwrap_or_else(|| {
                let (mut writer, count) = writing.expect("Empty ABA spool");
                writer.flush().expect("Failed to flush ABA outbox");
                let mut file = writer.into_inner().expect("Failed to reopen ABA outbox");
                file.rewind().expect("Failed to rewind ABA outbox");
                (BufReader::new(file), count)
            });
            let batch =
                bincode::deserialize_from(&mut reader).expect("Failed to read spooled ABA batch");
            let remaining = if count == 1 {
                None
            } else {
                Some((reader, count - 1))
            };
            (remaining, batch)
        })
        .await
        .expect("ABA spool reader stopped");
        self.reading = remaining;
        batch
    }
}
