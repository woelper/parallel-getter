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

extern crate crossbeam;
extern crate numtoa;
extern crate progress_streams;
extern crate reqwest;
extern crate tempfile;

mod range;

use crossbeam::thread::ScopedJoinHandle;
use numtoa::NumToA;
use progress_streams::ProgressWriter;
use reqwest::{Client, StatusCode, header::{
    CONTENT_LENGTH,
    CONTENT_RANGE,
    RANGE,
}};
use self::range::{calc_range, range_header, range_string};
use std::{fs, thread};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

trait PartWriter: Send + Seek + Read + Write {}
impl<T: Send + Seek + Read + Write> PartWriter for T {}

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
///     // Additional mirrors that can be used.
///     .mirrors(&["mirror_a", "mirror_b"])
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
    urls: Vec<&'a str>,
    dest: &'a mut W,
    threads: usize,
    threshold_parallel: Option<u64>,
    threshold_memory: Option<u64>,
    tries: u32,
    cache_path: Option<PathBuf>,
    progress_callback: Option<(Box<dyn Fn(u64, u64) -> bool>, u64)>,
    headers: Option<Arc<Vec<[String; 2]>>>,
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
            urls: vec![url],
            dest,
            threads: 4,
            threshold_parallel: None,
            threshold_memory: None,
            tries: 3,
            cache_path: None,
            progress_callback: None,
            headers: None,
        }
    }

    /// Submit the parallel GET request.
    pub fn get(mut self) -> io::Result<usize> {
        let client = self.client.take().unwrap_or_else(|| Arc::new(Client::new()));
        let length = match get_content_length(&client, self.urls[0]) {
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
    pub fn callback<F: Fn(u64, u64) -> bool + 'static>(mut self, poll_ms: u64, func: F) -> Self {
        self.progress_callback = Some((Box::new(func), poll_ms));
        self
    }

    /// Allow the caller to provide their own `Client`.
    pub fn client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Specify additional URLs that point to the same content, to be used for boosted transfers.
    pub fn mirrors(mut self, mirrors: &'a [&'a str]) -> Self {
        self.urls.truncate(1);
        self.urls.extend_from_slice(mirrors);
        self
    }

    /// Number of attempts to make before giving up.
    pub fn retries(mut self, tries: u32) -> Self {
        self.tries = tries;
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

    /// aditional headers to be set on requests
    pub fn headers(mut self, headers: Arc<Vec<[String; 2]>>) -> Self {
        self.headers = Some(headers);
        self
    }

    fn create_parts(
        &self,
        cache_path: &Path,
        length: u64,
        nthreads: u64,
        url_hash: &str
    ) -> io::Result<Vec<Box<dyn PartWriter>>> {
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

        Ok(parts)
    }

    fn parallel_get(&mut self, client: Arc<Client>, length: u64) -> io::Result<usize> {
        let progress = Arc::new(AtomicUsize::new(0));
        let nthreads = self.threads as u64;

        let has_callback = self.progress_callback.is_some();
        let progress_callback = self.progress_callback.take();
        let tries = self.tries;
        let urls = &self.urls;
        let headers = &self.headers;

        let tempdir;
        let cache_path = match self.cache_path {
            Some(ref path) => path.as_path(),
            None => {
                tempdir = tempfile::tempdir()?;
                tempdir.path()
            }
        };

        let url_hash = hash(urls[0]);
        let parts = self.create_parts(cache_path, length, nthreads, &url_hash)?;
        let dest = &mut self.dest;

        let result: io::Result<()> = {
            let progress = progress.clone();
            let cancel = Arc::new(AtomicBool::new(false));
            crossbeam::scope(move |scope| {
                let mut threads = Vec::new();
                for (part, mut part_file) in (0u64..nthreads).zip(parts.into_iter()) {
                    let client = client.clone();
                    let progress = progress.clone();

                    let local_progress = Arc::new(AtomicUsize::new(0));
                    let local = local_progress.clone();
                    let cancel = cancel.clone();
                    let handle: ScopedJoinHandle<io::Result<_>> = scope.spawn(move || {
                        let range = calc_range(length, nthreads, part);

                        for tried in 0..tries {
                            let url = urls[((part as usize) + tried as usize) % urls.len()];
                            let result = attempt_get(
                                &cancel,
                                client.clone(), &mut part_file, url, &progress,
                                &local, length, range, has_callback,
                                headers.clone()
                            );

                            match result {
                                Ok(()) => break,
                                Err(why) => {
                                    if cancel.load(Ordering::SeqCst) {
                                        return Err(why);
                                    }

                                    progress.fetch_sub(local.load(Ordering::SeqCst), Ordering::SeqCst);
                                    local.store(0, Ordering::SeqCst);
                                    part_file.seek(SeekFrom::Start(0))?;
                                    if tried == tries-1 {
                                        return Err(why);
                                    }
                                }
                            }
                        }

                        part_file.seek(SeekFrom::Start(0))?;

                        Ok(part_file)
                    });

                    threads.push(handle);
                }

                if let Some((ref progress_callback, mut poll_ms)) = progress_callback {
                    
                    // Poll for progress until all background threads have exited.
                    poll_ms = if poll_ms == 0 { 1 } else { poll_ms };
                    let poll_ms = Duration::from_millis(poll_ms);

                    let mut time_since_update = Instant::now();
                    while Arc::strong_count(&progress) != 1 {
                        let progress = progress.load(Ordering::SeqCst) as u64;
                        if time_since_update.elapsed() > poll_ms {
                            if progress_callback(progress, length) {
                                cancel.store(true, Ordering::SeqCst);
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "download cancelled"
                                ));
                            }
                            time_since_update = Instant::now();
                        }

                        if progress >= length { break }
                        thread::sleep(Duration::from_millis(1));
                    }
                }

                for handle in threads {
                    let mut file = handle.join().unwrap()?;
                    io::copy(&mut file, dest)?;
                }

                Ok(())
            })
        };

        result?;

        Ok(progress.load(Ordering::SeqCst))
    }
}

fn attempt_get(
    cancel: &Arc<AtomicBool>,
    client: Arc<Client>,
    part: &mut Box<dyn PartWriter>,
    url: &str,
    progress: &AtomicUsize,
    local: &AtomicUsize,
    length: u64,
    range: Option<(u64, u64)>,
    has_callback: bool,
    headers: Option<Arc<Vec<[String; 2]>>>
) -> io::Result<()> {
    if has_callback {
        let mut writer = WriteCancel::new(cancel.clone(), ProgressWriter::new(part, |written| {
            progress.fetch_add(written, Ordering::SeqCst);
            local.fetch_add(written, Ordering::SeqCst);
        }));

        send_get_request(client, &mut writer, url, length, range, headers)?;
    } else {
        send_get_request(client, part, url, length, range, headers)?;
    }

    Ok(())
}

fn send_get_request(
    client: Arc<Client>,
    writer: &mut impl Write,
    url: &str,
    length: u64,
    range: Option<(u64, u64)>,
    headers: Option<Arc<Vec<[String; 2]>>>
) -> io::Result<u64> {
    let mut client = client.get(url);

    if let Some((from, to)) = range {
        client = client.header(RANGE, range_string(from, to).as_str());
    }

    if let Some(headers) = headers {
        for h in &*headers {
            client = client.header(h[0].as_str(), h[1].as_str());
        }
    }

    let mut response = client.send()
        .map_err(|why| io::Error::new(
            io::ErrorKind::Other,
            format!("reqwest get failed: {}", why))
        )?;

    if let Some((from, to)) = range {
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "server did not return a partial request"
            ));
        }

        match response.headers().get(CONTENT_RANGE) {
            Some(header) => range_header(header, from, to, length)?,
            None => return Err(io::Error::new(
                io::ErrorKind::Other,
                "server did not return a `content-range` header"
            ))
        }
    } else {
        let status = response.status();
        if status != StatusCode::OK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("server returned an error: {}", status)
            ));
        }
    }

    response.copy_to(writer)
        .map_err(|why|
            io::Error::new(
                io::ErrorKind::Other,
                format!("reqwest copy failed: {}", why)
            )
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

struct WriteCancel<W> {
    cancel: Arc<AtomicBool>,
    writer: W,
}

impl<W: Write> WriteCancel<W> {
    pub fn new(cancel: Arc<AtomicBool>, writer: W) -> Self {
        Self { cancel, writer }
    }
}

impl<W: Write> Write for WriteCancel<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.cancel.load(Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "write cancelled"
            ))
        }

        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
