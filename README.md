# parallel-getter

When fetching a file from a web server via GET, it is possible to define a range of bytes to
receive per request. This allows the possibility of using multiple GET requests on the same
URL to increase the throughput of a transfer for that file. Once the parts have been fetched,
they are concatenated into a single file.

Therefore, this crate will make it trivial to set up a parallel GET request, with an API that
provides a configurable number of threads and an optional callback to monitor the progress of
a transfer.

## Example

```rust
extern crate parallel_getter;

use parallel_getter::ParallelGetter;
use std::fs::File;

fn main() {
    let url = "http://apt.pop-os.org/proprietary/pool/bionic/main/binary-amd64/a/atom/atom_1.31.1_amd64.deb";
    let mut file = File::create("atom_1.31.1_amd64.deb").unwrap();
    let result = ParallelGetter::new(url, &mut file)
        .threads(4)
        .callback(1000, Box::new(|p, t| {
            println!("{} KiB out of {} KiB", p / 1024, t / 1024);
        }))
        .get();

    if let Err(why) = result {
        eprintln!("errored: {}", why);
    }
}
```