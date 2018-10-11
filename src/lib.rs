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

extern crate numtoa;
extern crate progress_streams;
extern crate reqwest;
extern crate tempfile;

use numtoa::NumToA;
use progress_streams::ProgressWriter;
use reqwest::{Client, header::{
    CONTENT_LENGTH,
    CONTENT_RANGE,
    RANGE,
}};
use std::thread::{self, JoinHandle};
use std::fs;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
///     // threshold (length in bytes) to determine when multiple threads are required.
///     .threshold_parallel(1 * 1024 * 1024)
///     // threshold for defining when to store parts in memory or on disk.
///     .threshold_memory(10 * 1024 * 1024)
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
    threshold_parallel: Option<u64>,
    threshold_memory: Option<u64>,
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
            threshold_parallel: None,
            threshold_memory: None,
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
                        if let Some(threshold) = self.threshold_parallel {
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

        let result = self.parallel_get(client, length);

        // Ensure that the progress is completed when returning.
        if let Some((callback, _)) = self.progress_callback {
            callback(length, length);
        }

        result
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
    pub fn threshold_memory(mut self, length: u64) -> Self {
        self.threshold_memory = Some(length);
        self
    }

    /// If the length is less than this threshold, threads will not be used.
    pub fn threshold_parallel(mut self, bytes: u64) -> Self {
        self.threshold_parallel = Some(bytes);
        self
    }

    /// Number of threads to download a file with.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    fn parallel_get(&mut self, client: Arc<Client>, length: u64) -> io::Result<usize> {
        let mut threads = Vec::new();
        let progress = Arc::new(AtomicUsize::new(0));
        let nthreads = self.threads as u64;
        let has_callback = self.progress_callback.is_some();

        let tempdir;
        let cache_path = match self.cache_path {
            Some(ref path) => path.as_path(),
            None => {
                tempdir = tempfile::tempdir()?;
                tempdir.path()
            }
        };

        let url_hash = hash(self.url);
        let mut parts = Vec::new();

        for part in 0u64..nthreads {
            let mut part: Box<dyn PartWriter> = match self.threshold_memory {
                Some(threshold) if threshold > length => {
                    Box::new(Cursor::new(
                        Vec::with_capacity((length / nthreads) as usize)
                    ))
                }
                _ => {
                    let mut array = [0u8; 20];
                    let name = [&url_hash, "-", part.numtoa_str(10, &mut array)].concat();
                    let tempfile = cache_path.join(&name);
                    Box::new(
                        fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .truncate(true)
                            .create(true)
                            .open(tempfile)?
                    )
                }
            };

            parts.push(part);
        }

        for (part, mut part_file) in (0u64..nthreads).zip(parts.into_iter()) {
            let client = client.clone();
            let url = self.url.to_owned();
            let progress = progress.clone();

            let handle: JoinHandle<io::Result<_>> = thread::spawn(move || {
                let range = get_range(length, nthreads, part);

                if has_callback {
                    let mut writer = ProgressWriter::new(&mut part_file, |written| {
                        progress.fetch_add(written, Ordering::SeqCst);
                    });

                    send_get_request(client, &mut writer, &url, range)?;
                } else {
                    send_get_request(client, &mut part_file, &url, range)?;
                }

                part_file.seek(SeekFrom::Start(0))?;

                Ok(part_file)
            });

            threads.push(handle);
        }

        if let Some((ref progress_callback, mut poll_ms)) = self.progress_callback {
            // Poll for progress until all background threads have exited.
            poll_ms = if poll_ms == 0 { 1 } else { poll_ms };
            let poll_ms = Duration::from_millis(poll_ms);

            let mut time_since_update = Instant::now();
            while Arc::strong_count(&progress) != 1 {
                let progress = progress.load(Ordering::SeqCst) as u64;
                if time_since_update.elapsed() > poll_ms {
                    progress_callback(progress, length);
                }
                
                if progress >= length { break }
                thread::sleep(Duration::from_millis(1));
            }
        }

        for handle in threads {
            let mut file = handle.join().unwrap()?;
            io::copy(&mut file, self.dest)?;
        }

        Ok(progress.load(Ordering::SeqCst))
    }
}

trait PartWriter: Send + Seek + Read + Write {}
impl<T: Send + Seek + Read + Write> PartWriter for T {}

fn get_range_string(from: u64, to: u64) -> String {
    let mut from_a = [0u8; 20];
    let mut to_a = [0u8; 20];
    [
        "bytes=",
        from.numtoa_str(10, &mut from_a),
        "-",
        to.numtoa_str(10, &mut to_a)
    ].concat()
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
        client = client.header(RANGE, get_range_string(from, to).as_str());
    }

    let mut response = client.send()
        .map_err(|why| io::Error::new(
            io::ErrorKind::Other,
            format!("reqwest get failed: {}", why))
        )?;

    response.copy_to(writer).map_err(|why|
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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn hash(string: &str) -> String {
    let mut hasher = DefaultHasher::new();
    string.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}
