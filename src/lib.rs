extern crate tar;
extern crate tokio;

use futures::future::poll_fn;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs as tokio_fs;
use tokio::prelude::*;

const EMPTY_BLOCK: [u8; 512] = [0; 512];

enum TarState {
    BeforeNext,
    NextFile {
        path: PathBuf,
    },
    OpeningFile {
        file: tokio_fs::file::OpenFuture<PathBuf>,
        fname: OsString,
    },
    PrepareHeader {
        fname: OsString,
        meta: tokio_fs::file::MetadataFuture,
    },
    HeaderReady {
        file: tokio_fs::File,
        fname: OsString,
        meta: fs::Metadata,
    },
    Sending {
        file: tokio_fs::File,
    },

    Finish {
        block: u8,
    },
}

pub struct TarStream {
    state: Option<TarState>,
    iter: Box<dyn Iterator<Item = PathBuf> + Send>,
    position: usize,
    buf: [u8; 8 * 1024],
}

impl TarStream {
    pub fn tar_dir<P: AsRef<Path> + Send>(
        dir: P,
    ) -> Box<Future<Item = Self, Error = io::Error> + Send> {
        let dir: PathBuf = dir.as_ref().to_owned();
        let dir = tokio_fs::read_dir(dir).flatten_stream();

        let files = dir
            .and_then(|entry| {
                let path = entry.path();
                poll_fn(move || entry.poll_file_type()).map(|file_type| (path, file_type))
            })
            .filter_map(|(path, file_type)| {
                if file_type.is_file() {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        let ts = files.map(|paths| {
            let iter = paths.into_iter();
            let state = Some(TarState::BeforeNext);
            TarStream {
                state,
                iter: Box::new(iter),
                position: 0,
                buf: [0; 8 * 1024],
            }
        });

        Box::new(ts)
    }
}

impl Stream for TarStream {
    type Item = Vec<u8>;
    type Error = io::Error;
    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        loop {
            match self.state.take() {
                None => break,
                Some(state) => {
                    match state {
                        // move to next file
                        TarState::BeforeNext => match self.iter.next() {
                            None => {
                                self.state = Some(TarState::Finish { block: 0 });
                            }
                            Some(path) => {
                                self.state = Some(TarState::NextFile { path });
                            }
                        },
                        // we start with async opening of file
                        TarState::NextFile { path } => {
                            let fname = path.file_name().map(|name| name.to_owned()).unwrap();
                            let file = tokio_fs::File::open(path);
                            self.state = Some(TarState::OpeningFile { file, fname });
                        }

                        // now test if file is opened
                        TarState::OpeningFile { mut file, fname } => match file.poll() {
                            Ok(Async::NotReady) => {
                                self.state = Some(TarState::OpeningFile { file, fname });
                                return Ok(Async::NotReady);
                            }
                            Ok(Async::Ready(file)) => {
                                let meta = file.metadata();
                                self.state = Some(TarState::PrepareHeader { fname, meta })
                            }

                            Err(e) => return Err(e),
                        },

                        //when file is opened read its metadata
                        TarState::PrepareHeader { fname, mut meta } => match meta.poll() {
                            Ok(Async::NotReady) => {
                                self.state = Some(TarState::PrepareHeader { fname, meta });
                                return Ok(Async::NotReady);
                            }

                            Ok(Async::Ready((file, meta))) => {
                                self.state = Some(TarState::HeaderReady { file, fname, meta });
                            }

                            Err(e) => return Err(e),
                        },

                        // from metadata create tar header
                        TarState::HeaderReady { file, fname, meta } => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            let mut header = tar::Header::new_gnu();
                            header.set_path(fname).expect("cannot set path in header");
                            header.set_size(meta.len());
                            header.set_mode(0o644);
                            header.set_mtime(now);
                            header.set_cksum();
                            let bytes = header.as_bytes();
                            let chunk = bytes.to_vec();
                            self.state = Some(TarState::Sending { file });
                            self.position = 0;
                            return Ok(Async::Ready(Some(chunk)));
                        }

                        // and send file data into stream
                        TarState::Sending { mut file } => {
                            match file.poll_read(&mut self.buf[self.position..]) {
                                Ok(Async::NotReady) => {
                                    self.state = Some(TarState::Sending { file });
                                    return Ok(Async::NotReady);
                                }

                                Ok(Async::Ready(read)) => {
                                    if read == 0 {
                                        self.state = Some(TarState::BeforeNext);
                                        if self.position > 0 {
                                            let rem = self.position % 512;
                                            let padding_length =
                                                if rem > 0 { 512 - rem } else { 0 };
                                            let new_position = self.position + padding_length;
                                            // zeroing padding, hoping compiler can optimize
                                            for i in &mut self.buf[self.position..new_position] {
                                                *i = 0
                                            }
                                            return Ok(Async::Ready(Some(
                                                self.buf[..new_position].to_vec(),
                                            )));
                                        }
                                    } else {
                                        self.position += read;
                                        self.state = Some(TarState::Sending { file });
                                        if self.position == self.buf.len() {
                                            let chunk = self.buf[..self.position].to_vec();
                                            self.position = 0;
                                            return Ok(Async::Ready(Some(chunk)));
                                        }
                                    }
                                }

                                Err(e) => return Err(e),
                            }
                        }
                        // tar format requires two empty blocks at the end
                        TarState::Finish { block } => {
                            if block < 2 {
                                let chunk = EMPTY_BLOCK.to_vec();
                                self.state = Some(TarState::Finish { block: block + 1 });
                                return Ok(Async::Ready(Some(chunk)));
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(Async::Ready(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::codec::Decoder;

    #[test]
    fn create_tar() {
        let tar = TarStream::tar_dir(".");
        let temp_dir = tempdir().unwrap();
        let tar_file_name = temp_dir.path().join("test.tar");
        //let tar_file_name = Path::new("/tmp/test.tar");
        let tar_file_name2 = tar_file_name.clone();

        // create tar file asychronously
        {
            let tar_file = tokio_fs::File::create(tar_file_name);
            let f = tar
                .and_then(move |tar_stream| {
                    tar_file.and_then(|f| {
                        let codec = tokio::codec::BytesCodec::new();
                        let file_sink = codec.framed(f);
                        file_sink.send_all(tar_stream.map(|v| v.into()))
                    })
                })
                .map(|_r| ())
                .map_err(|e| eprintln!("Error during tar creation: {}", e));

            tokio::run(f);
        }


        
        let mut ar = tar::Archive::new(fs::File::open(tar_file_name2).unwrap());

        let entries = ar.entries().unwrap();
        let mut count = 0;
        for entry in entries {
            let mut entry = entry.unwrap();
            let p = entry.path().unwrap().into_owned();

            let mut data_from_archive = vec![];
            let mut data_from_file = vec![];
            entry.read_to_end(&mut data_from_archive).unwrap();
            {
                let mut f = fs::File::open(&p).unwrap();
                f.read_to_end(&mut data_from_file).unwrap();
            }

            println!(
                "File {:?} entry header start {}, file start {}",
                p,
                entry.raw_header_position(),
                entry.raw_file_position()
            );
            println!(
                "File {:?} archive len {}, file len {}",
                p,
                data_from_archive.len(),
                data_from_file.len()
            );

            assert_eq!(
                data_from_archive.len(),
                data_from_file.len(),
                "File len {:?}",
                p
            );

            count+=1;
        }

        assert_eq!(3, count, "There are 3 files in dir");
        temp_dir.close().unwrap();
    }

}
