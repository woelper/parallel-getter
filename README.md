# parallel-getter

When fetching a file from a web server via GET, it is possible to define a range of bytes to
receive per request. This allows the possibility of using multiple GET requests on the same
URL to increase the throughput of a transfer for that file. Once the parts have been fetched,
they are concatenated into a single file.

Therefore, this crate will make it trivial to set up a parallel GET request, with an API that
provides a configurable number of threads and an optional callback to monitor the progress of
a transfer.

## Features

- Download a file with parallel GET requests to maximize bandwidth usage.
- Specify mirrors to spread requests out to separate servers.
- Control the number of threads to use to fetch a request.
- Define a minimum threshold in size for storing parts in memory.
- Define a minimum threshold for when threads should be used.
- Specify a number of attempts to be made before giving up.
- An optional callback for monitoring progress of a download.
- Fall back to a single thread if the server does not support ranges.
- Fall back to a single thread if the server does not return a content length.
- Alternate between mirrors when retrying after a failure.

## Example

```rust
extern crate reqwest;
extern crate parallel_getter;

use reqwest::Client;
use parallel_getter::ParallelGetter;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

let client = Arc::new(Client::new());
let mut file = File::create("new_file").unwrap();
ParallelGetter::new("url_here", &mut file)
    // Additional mirrors that can be used.
    .mirrors(&["mirror_a", "mirror_b"])
    // Optional client to use for the request.
    .client(client)
    // Optional path to store the parts.
    .cache_path(PathBuf::from("/a/path/here"))
    // Number of theads to use.
    .threads(5)
    // threshold (length in bytes) to determine when multiple threads are required.
    .threshold_parallel(1 * 1024 * 1024)
    // threshold for defining when to store parts in memory or on disk.
    .threshold_memory(10 * 1024 * 1024)
    // Callback for monitoring progress.
    .callback(16, Box::new(|progress, total| {
        println!(
            "{} of {} KiB downloaded",
            progress / 1024,
            total / 1024
        );
    }))
    // Commit the parallel GET requests.
    .get();
```