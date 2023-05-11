use fst::{Map, MapBuilder, IntoStreamer, Streamer};
use memmap::Mmap;
use crate::vecmap::{VecMap, unpack};
use std::{fs, io, mem::size_of_val, fs::File, fs::OpenOptions};
pub struct FstMmap {
    pub fst_map: Map<Mmap>,
    items: VecMap
}

impl FstMmap {
    pub fn from_vec<K: AsRef<[u8]> + Ord + Clone>(path: &str, data: impl Iterator<Item=(K, Vec<u8>)>) -> std::io::Result<Self> {
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
    pub fn get_less_or_equal<K: AsRef<[u8]>>(&self, key: K) -> Option<(Vec<u8>, &[u8])> {
        let mut stream = self.fst_map.range().le(key).into_stream();
        match stream.next(){
            None => {None}
            Some((key, packed)) => {
                let (start, end) = unpack(packed);
                self.items.get_bytes(start as usize, (end - start) as usize).map(|val| (key.to_vec(), val))
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