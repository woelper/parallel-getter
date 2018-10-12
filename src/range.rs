use numtoa::NumToA;
use reqwest::header::HeaderValue;
use std::io;


pub(crate) fn calc_range(length: u64, nthreads: u64, part: u64) -> Option<(u64, u64)> {
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

fn parse_range(header: &str) -> io::Result<(u64, u64, u64)> {
    if header.starts_with("bytes ") {
        let header = &header[6..];
        if let (Some(tpos), Some(npos)) = (header.rfind('/'), header.find('-')) {
            let from = &header[..npos];
            let to = &header[npos+1..tpos];
            let total = &header[tpos+1..];
            return from.parse::<u64>()
                .and_then(|f| to.parse::<u64>().map(|t| (f, t)))
                .and_then(|(f, t)| total.parse::<u64>().map(|m| (f, t, m)))
                .map_err(|_| io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("content range did not contain a number")
                ));
        }
    }

    Err(io::Error::new(io::ErrorKind::InvalidData, "content-range header is corrupt"))
}

pub(crate) fn range_header(header: &HeaderValue, from: u64, to: u64, length: u64) -> io::Result<()> {
    let header = header.to_str().map_err(|_| io::Error::new(
        io::ErrorKind::InvalidData,
        "received range header was not UTF-8"
    ))?;

    let (pfrom, pto, plen) = parse_range(header)?;
    if from != pfrom || to != pto {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("server responded with an incorrect range: \
                found {}-{}; expected {}-{}",
                pfrom, pto, from, to
            )
        ));
    }

    if plen != length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("server responded with an incorrect length: \
                found {}; expected {}",
                plen, length
            )
        ));
    }

    Ok(())
}

pub(crate) fn range_string(from: u64, to: u64) -> String {
    let mut from_a = [0u8; 20];
    let mut to_a = [0u8; 20];
    [
        "bytes=",
        from.numtoa_str(10, &mut from_a),
        "-",
        to.numtoa_str(10, &mut to_a)
    ].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_range_test() {
        let example = "bytes 124125-1215251/195125810";
        assert_eq!(
            (124125, 1215251, 195125810),
            parse_range(example).unwrap()
        );

        assert!(parse_range("").is_err());
        assert!(parse_range("bytes ").is_err());
        assert!(parse_range("bytes 12412").is_err());
        assert!(parse_range("bytes 122151-").is_err());
        assert!(parse_range("bytes 122151-125125125").is_err());
        assert!(parse_range("bytes 122151-224151/").is_err());
        assert!(parse_range("bytes 122151-325235235/a").is_err());
    }
}