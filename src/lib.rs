//! When fetching a file from a web server via GET, it is possible to define a range of bytes to
//! receive per request. This allows the possibility of using multiple GET requests on the same
//! URL to increase the throughput of a transfer for that file. Once the parts have been fetched,
//! they are concatenated into a single file.
//! 
//! Therefore, this crate will make it trivial to set up a parallel GET request, with an API that
//! provides a configurable number of threads and an optional callback to monitor the progress of
//! a transfer.
//! 
//! ## Example
//! 
//! ```rust,no_run
//! extern crate parallel_getter;
//! 
//! use parallel_getter::ParallelGetter;
//! use std::fs::File;
//! 
//! fn main() {
//!     let url = "http://apt.pop-os.org/proprietary/pool/bionic/main/\
//!         binary-amd64/a/atom/atom_1.31.1_amd64.deb";
//!     let mut file = File::create("atom_1.31.1_amd64.deb").unwrap();
//!     let result = ParallelGetter::new(url, &mut file)
//!         .threads(4)
//!         .callback(1000, Box::new(|p, t| {
//!             println!(
//!                 "{} of {} KiB downloaded",
//!                 p / 1024,
//!                 t / 1024);
//!         }))
//!         .get();
//! 
//!     if let Err(why) = result {
//!         eprintln!("errored: {}", why);
//!     }
//! }
//! ```

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

/// Type for constructing parallel GET requests.
/// 
/// # Example
/// 
/// ```rust,no_run
/// extern crate reqwest;
/// extern crate parallel_getter;
/// 
/// use reqwest::Client;
/// use parallel_getter::ParallelGetter;
/// use std::fs::File;
/// use std::path::PathBuf;
/// use std::sync::Arc;
/// 
/// let client = Arc::new(Client::new());
/// let mut file = File::create("new_file").unwrap();
/// ParallelGetter::new("url_here", &mut file)
///     // Optional client to use for the request.
///     .client(client)
///     // Optional path to store the parts.
///     .cache_path(PathBuf::from("/a/path/here"))
///     // Number of theads to use.
///     .threads(5)
///     // threshold in file length for using parallel requests.
///     .parallel_threshold(1 * 1024 * 1024)
///     // Callback for monitoring progress.
///     .callback(16, Box::new(|progress, total| {
///         println!(
///             "{} of {} KiB downloaded",
///             progress / 1024,
///             total / 1024
///         );
///     }))
///     // Commit the parallel GET requests.
///     .get();
/// ```
pub struct ParallelGetter<'a, W: Write + 'a> {
    client: Option<Arc<Client>>,
    url: &'a str,
    dest: &'a mut W,
    length: Option<u64>,
    threads: usize,
    parallel_threshold: Option<u64>,
    memory_threshold: Option<u64>,
    cache_path: Option<PathBuf>,
    progress_callback: Option<(Box<Fn(u64, u64)>, u64)>
}

impl<'a, W: Write> ParallelGetter<'a, W> {
    /// Initialize a new `ParallelGetter`
    /// 
    /// # Notes
    /// - The `url` is the location which we will send a GET to.
    /// - The `dest` is where the completed file will be written to.
    /// - By default, 4 threads will be used.
    pub fn new(url: &'a str, dest: &'a mut W) -> Self {
        Self {
            client: None,
            url,
            dest,
            length: None,
            threads: 4,
            parallel_threshold: None,
            memory_threshold: None,
            cache_path: None,
            progress_callback: None,
        }
    }

    /// Submit the parallel GET request.
    pub fn get(mut self) -> io::Result<usize> {
        let client = self.client.take().unwrap_or_else(|| Arc::new(Client::new()));
        let length = match self.length {
            Some(length) => length,
            None => {
                match get_content_length(&client, self.url) {
                    Ok(length) => {
                        if let Some(threshold) = self.parallel_threshold {
                            if length < threshold {
                                self.threads = 1;
                            }
                        }

                        length
                    },
                    Err(_) => {
                        self.threads = 1;
                        0
                    }
                }
            }
        };

        self.parallel_get(client, length)
    }

    /// If defined, downloads will be stored here instead of a temporary directory.
    /// 
    /// # Notes
    /// This is required to enable resumable downloads (not yet implemented).
    pub fn cache_path(mut self, path: PathBuf) -> Self {
        self.cache_path = Some(path);
        self
    }

    /// Specify a callback for monitoring progress, to be polled at the given interval.
    pub fn callback(mut self, poll_ms: u64, func: Box<Fn(u64, u64)>) -> Self {
        self.progress_callback = Some((func, poll_ms));
        self
    }

    /// Allow the caller to provide their own `Client`.
    pub fn client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Specifies the length of the file, to avoid a HEAD request to find out.
    pub fn length(mut self, bytes: u64) -> Self {
        self.length = Some(bytes);
        self
    }

    /// If the length is less than this threshold, parts will be stored in memory.
    pub fn memory_threshold(mut self, length: u64) -> Self {
        self.memory_threshold = Some(length);
        self
    }

    /// If the length is less than this threshold, threads will not be used.
    pub fn parallel_threshold(mut self, bytes: u64) -> Self {
        self.parallel_threshold = Some(bytes);
        self
    }

    /// Number of threads to download a file with.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    fn parallel_get(self, client: Arc<Client>, length: u64) -> io::Result<usize> {
        let mut threads = Vec::new();
        let progress = Arc::new(AtomicUsize::new(0));
        let nthreads = self.threads as u64;
        let has_callback = self.progress_callback.is_some();
        // let memory_threshold = self.memory_threshold;

        let tempdir;
        let cache_path = match self.cache_path {
            Some(ref path) => path.as_path(),
            None => {
                tempdir = tempfile::tempdir()?;
                tempdir.path()
            }
        };

        for part in 0u64..nthreads {
            let tempfile = cache_path.join(format!("{}", part));
            let client = client.clone();
            let url = self.url.to_owned();
            let progress = progress.clone();
            let handle: JoinHandle<io::Result<File>> = thread::spawn(move || {
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
            while Arc::strong_count(&progress) != 1 {
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
    if length == 0 || nthreads == 1 {
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
        .ok_or_else(|| io::Error::new(
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
