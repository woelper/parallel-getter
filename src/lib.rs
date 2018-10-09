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
use std::path::PathBuf;
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
    /// If the length is less than this threshold, threads will not be used.
    parallel_threshold: usize,
    /// If the length is less than this threshold, parts will be stored in memory.
    memory_threshold: usize,
    /// If defined, downloads will be stored here instead of a temporary directory.
    cache_path: Option<PathBuf>,
    /// Optional progress callback to track progress.
    progress_callback: Option<(Arc<Fn(u64, u64)>, u64)>
}

impl<'a, W: Write> ParallelGetter<'a, W> {
    pub fn new(url: &'a str, dest: &'a mut W) -> Self {
        Self {
            client: None,
            url,
            dest,
            length: None,
            threads: 4,
            parallel_threshold: 0,
            memory_threshold: 0,
            cache_path: None,
            progress_callback: None,
        }
    }

    pub fn callback(mut self, poll_ms: u64, func: Arc<Fn(u64, u64)>) -> Self {
        self.progress_callback = Some((func, poll_ms));
        self
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

    pub fn get(mut self) -> io::Result<usize> {
        let client = self.client.take().unwrap_or_else(|| Arc::new(Client::new()));
        let length = match self.length {
            Some(length) => length,
            None => {
                match get_content_length(&client, self.url) {
                    Ok(length) => length,
                    Err(_) => {
                        self.threads = 1;
                        0
                    }
                }
            }
        };

        self.parallel_get(client, length)
    }

    fn parallel_get(self, client: Arc<Client>, length: u64) -> io::Result<usize> {
        let mut threads: Vec<JoinHandle<io::Result<File>>> = Vec::new();
        let progress = Arc::new(AtomicUsize::new(0));
        let threads_running = Arc::new(());
        let nthreads = self.threads as u64;
        let tempdir = tempfile::tempdir()?;
        let has_callback = self.progress_callback.is_some();

        for part in 0u64..nthreads {
            let running = threads_running.clone();
            let tempfile = tempdir.path().join(format!("{}", part));
            let client = client.clone();
            let url = self.url.to_owned();
            let progress = progress.clone();
            let handle = thread::spawn(move || {
                let _running = running;
                let range = get_range(length, nthreads, part);
                
                let mut part = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(tempfile)?;

                if has_callback {
                    let mut writer = ProgressWriter::new(part, |written| {
                        progress.fetch_add(written, Ordering::SeqCst);
                    });

                    send_get_request(client, &mut writer, &url, range)?;
                    part = writer.into_inner();
                } else {
                    send_get_request(client, &mut part, &url, range)?;
                }

                part.seek(SeekFrom::Start(0))?;

                Ok(part)
            });

            threads.push(handle);
        }

        if let Some((progress_callback, mut poll_ms)) = self.progress_callback {
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

fn get_range(length: u64, nthreads: u64, part: u64) -> Option<(u64, u64)> {
    if length == 0 {
        None
    } else {
        let section = length / nthreads;
        let from = part * section;
        let to = if part + 1 == nthreads {
            length
        } else {
            (part + 1) * section
        } - 1;
        Some((from, to))
    }
}

fn send_get_request(client: Arc<Client>, writer: &mut impl Write, url: &str, range: Option<(u64, u64)>) -> io::Result<u64> {
    let mut client = client.get(url);

    if let Some((from, to)) = range {
        client = client.header(RANGE, format!("bytes={}-{}", from, to));
    }
        
    client.send()
        .map_err(|why| io::Error::new(io::ErrorKind::Other, format!("reqwest get failed: {}", why)))?
        .copy_to(writer).map_err(|why|
            io::Error::new(io::ErrorKind::Other,
            format!("reqwest copy failed: {}", why))
        )
}

fn get_content_length(client: &Client, url: &str) -> io::Result<u64> {
    let resp = client.head(url).send()
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
        ))
}