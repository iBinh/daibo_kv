use fst::{Map, MapBuilder, IntoStreamer, Streamer, raw::Output};
use memmap::Mmap;
use crate::vecmap::{VecMap, unpack};
use std::{fs, io, mem::size_of_val, fs::File, fs::OpenOptions, f32::consts::E};
pub struct FstMmap {
    pub fst_map: Map<Mmap>,
    items: VecMap
}

impl FstMmap {
    pub fn from_iter<K: AsRef<[u8]> + Ord + Clone>(path: &str, data: impl IntoIterator<Item=(K, Vec<u8>)>) -> std::io::Result<Self> {
        fs::create_dir(path).expect("path already exists");
        let mut vecmap_path = path.to_string();
        vecmap_path.push_str("/data");
        let mut fst_path = path.to_string();
        fst_path.push_str("/fst");
        let cap = 4;
        let mut items = VecMap::with_capacity(&vecmap_path, cap)?;
        let mut fst_input: Vec<(K, u64)> = Vec::new();
        for datum in data {
            let push_result = items.push(&datum.1)?;
            fst_input.push((datum.0, push_result));
        }
        items.mmap.flush_async()?;
        let wtr = io::BufWriter::new(File::create(&fst_path)?);
        let mut builder = MapBuilder::new(wtr).unwrap();
        fst_input.sort();
        fst_input.dedup_by(|a, b| a.0 == b.0);
        for elem in fst_input {
            builder.insert(elem.0, elem.1).unwrap();
        }
        builder.finish().unwrap();
        let fst_file = OpenOptions::new().read(true).append(true).create(true).open(fst_path)?;
        let fst_map = unsafe {Map::new(Mmap::map(&fst_file).unwrap()).unwrap()};
        Ok(
            FstMmap{
                fst_map,
                items
            }
        )       
    }
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]> {
        match self.fst_map.get(key){
            None => {None}
            Some(packed) => {
                let (start, end) = unpack(packed);
                self.items.get_bytes(start as usize, (end - start) as usize)
            }
        }
    }

    #[inline]
    fn get_le(&self, key: &[u8]) -> Option<Output> {
        let fst = self.fst_map.as_fst();
        let mut node = fst.root();
        let mut out = Output::zero();
        for &b in key {
            match node.find_input(b) {
                None => {
                    let mut greatest_less_than = 0;
                    for (i, transition) in node.transitions().enumerate() {
                        if transition.inp > b {
                            if i == 0 {
                                return None
                            }
                            break;
                        }
                        greatest_less_than = i;
                    }
                    let t = node.transition(greatest_less_than);
                    out = out.cat(t.out);
                    node = fst.node(t.addr);
                    while !node.is_final() {
                        let i = node.len() - 1;
                        let t = node.transition(i);
                        out = out.cat(t.out);
                        node = fst.node(t.addr);
                    }
                    return Some(out.cat(node.final_output()));
                },
                Some(i) => {
                    let t = node.transition(i);
                    out = out.cat(t.out);
                    node = fst.node(t.addr)
                }
            }
        }
        if !node.is_final() {
            None
        } else {
            Some(out.cat(node.final_output()))
        }
    }
    pub fn get_less_or_equal<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]> {
        match self.get_le(key.as_ref()) {
            None => {None}
            Some(packed) => {
                let (start, end) = unpack(packed.value());
                self.items.get_bytes(start as usize, (end - start) as usize)
            }
        }
    }
    pub fn from_path(path: &str) -> std::io::Result<Self> {
        let mut fst_path = path.to_string();
        fst_path.push_str("/fst");
        let mut vecmap_path = path.to_string();
        vecmap_path.push_str("/data");
        let items = VecMap::from_file(&vecmap_path)?;
        let fst_file = OpenOptions::new().read(true).append(true).create(true).open(fst_path)?;
        let fst_map = unsafe {Map::new(Mmap::map(&fst_file).unwrap()).unwrap()};
        Ok(
            FstMmap {
                fst_map,
                items
            }
        )
    }
}