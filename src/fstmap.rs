use fst::{IntoStreamer, Streamer, Map, MapBuilder};
#[derive(Clone, Debug)]
pub struct FstMap<T> {
    map: Map<Vec<u8>>,
    items: Vec<T>
}

impl <T> FstMap<T> {
    /// create FstMap from a Vector of key value tuples
    pub fn from_vec<K: AsRef<[u8]>>( mut pairs: Vec<(K, T)>) -> Self where K: std::cmp::Ord {
        let mut items: Vec<T> = Vec::with_capacity(pairs.len());
        let mut index_pairs: Vec<(K, u64)> = Vec::with_capacity(pairs.len());
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        for pair in pairs {
            items.push(pair.1);
            index_pairs.push((pair.0, items.len() as u64 - 1));
        }
        let map = Map::from_iter(index_pairs).unwrap();
        FstMap{
            map,
            items,
        }
    }
    /// get value by key
    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&T> {
        match self.map.get(key) {
            None => {return None},
            Some(val) => {
                Some(&self.items[val as usize])
            }
        }
    }
}