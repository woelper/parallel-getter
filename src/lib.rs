extern crate progress_streams;
extern crate reqwest;
extern crate tempfile;

use progress_streams::ProgressWriter;
use reqwest::{Client, header::{
    CONTENT_LENGTH,
    RANGE,
}};
use std::thread::{self, JoinHandle};
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub struct ParallelGetter<'a, W: Write + 'a> {
    client: Option<Arc<Client>>,
    url: &'a str,
    dest: &'a mut W,
    /// If no length is provided, a HEAD request will be used to fetch it.
    length: Option<u64>,
    /// Number of threads to download a file with.
    threads: usize,
    /// Optional progress callback to track progress.
    progress_callback: Option<(Box<FnMut(u64, u64)>, u64)>
}

impl<'a, W: Write> ParallelGetter<'a, W> {
    pub fn new(url: &'a str, dest: &'a mut W) -> Self {
        Self {
            client: None,
            url,
            dest,
            length: None,
            threads: 4,
            progress_callback: None,
        }
    }

    pub fn client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    pub fn callback(mut self, poll_ms: u64, func: Box<FnMut(u64, u64)>) -> Self {
        self.progress_callback = Some((func, poll_ms));
        self
    }

    pub fn get(mut self) -> io::Result<usize> {
        let client = self.client.take().unwrap_or_else(|| Arc::new(Client::new()));
        let nthreads = self.threads as u64;
        let progress = Arc::new(AtomicUsize::new(0));
        let threads_running = Arc::new(());
        let length = match self.length {
            Some(length) => length,
            None => {
                let resp = client.head(self.url).send()
                    .map_err(|why| io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to send HEAD reqest: {}", why)
                    ))?;

                resp.headers().get(CONTENT_LENGTH)
                    .ok_or(io::Error::new(
                        io::ErrorKind::NotFound,
                        "content length not found"
                    ))?
                    .to_str()
                    .map_err(|_| io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Content-Length is not UTF-8"
                    ))?
                    .parse::<u64>()
                    .map_err(|_| io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Content-Length did not have a valid u64 integer"
                    ))?
            }
        };

        let tempdir = tempfile::tempdir()?;
        let mut threads: Vec<JoinHandle<io::Result<File>>> = Vec::new();
        
        for part in 0u64..nthreads {
            let running = threads_running.clone();
            let tempfile = tempdir.path().join(format!("{}", part));
            let client = client.clone();
            let url = self.url.to_owned();
            let progress = progress.clone();
            let handle = thread::spawn(move || {
                let _running = running;
                let section = length / nthreads;
                let from = part * section;
                let to = if part + 1 == nthreads {
                    length
                } else {
                    (part + 1) * section
                } - 1;

                let part = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(tempfile)?;

                let mut writer = ProgressWriter::new(part, |written| {
                    progress.fetch_add(written, Ordering::SeqCst);
                });

                client
                    .get(&url)
                    .header(RANGE, format!("bytes={}-{}", from, to))
                    .send()
                    .map_err(|why| io::Error::new(io::ErrorKind::Other, format!("reqwest get failed: {}", why)))?
                    .copy_to(&mut writer).map_err(|why|
                        io::Error::new(io::ErrorKind::Other,
                        format!("reqwest copy failed: {}", why))
                    )?;

                let mut part = writer.into_inner();
                part.seek(SeekFrom::Start(0))?;

                Ok(part)
            });

            threads.push(handle);
        }

        if let Some((mut progress_callback, mut poll_ms)) = self.progress_callback {
            // Poll for progress until all background threads have exited.
            poll_ms = if poll_ms == 0 { 1 } else { poll_ms };
            while Arc::strong_count(&threads_running) != 1 {
                let progress = progress.load(Ordering::SeqCst) as u64;
                progress_callback(progress, length);
                if progress >= length { break }
                thread::sleep(Duration::from_millis(poll_ms));
            }
        }

        for handle in threads {
            let mut file = handle.join().unwrap()?;
            io::copy(&mut file, self.dest)?;
        }

        Ok(progress.load(Ordering::SeqCst))
    }
}