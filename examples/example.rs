extern crate parallel_getter;

use parallel_getter::ParallelGetter;

fn main() {
    let url = "http://apt.pop-os.org/proprietary/pool/bionic/main/binary-amd64/a/atom/atom_1.31.1_amd64.deb";

    let result = ParallelGetter::new(url, "atom_1.31.1_amd64.deb")
        .threads(4)
        .callback(1000, Box::new(|p, t| {
            println!("{} KiB out of {} KiB", p / 1024, t / 1024);
        }))
        .get();

    if let Err(why) = result {
        eprintln!("errored: {}", why);
    }
}