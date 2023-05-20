use fst::{Map, MapBuilder, raw::{Output, Node, Fst}};
use memmap::Mmap;
use crate::vecmap::{VecMap, unpack};
use std::{fs, io, fs::File, fs::OpenOptions};
pub struct FstMmap {
    pub fst_map: Map<Mmap>,
    items: VecMap
}
type Backtrack<'a> = Option<(Node<'a>, Output, usize)>;
#[derive(Debug, Clone, Copy)]
struct CompareNext<'a> {
    node: Node<'a>,
    output: Output,
    backtrack: Backtrack<'a>,
    input: &'a [u8],
}
#[derive(Debug, Clone, Copy)]
struct ChooseBiggest<'a> {
    node: Node<'a>,
    output: Output,
}
#[derive(Debug, Clone, Copy)]
enum Outcome<'a> {
    CompareNext(CompareNext<'a>),
    ChooseBiggest(ChooseBiggest<'a>),
    /// Go back to the first byte that is lowest, then ChooseBiggest
    Backtrack(Backtrack<'a>),
    /// Key is lower than every key in the map, abort the search
    Abort,
    /// Reached a final state, output contains the value
    Final(Output),
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
    #[inline]
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
    fn backtrack<'a>(raw: &'a Fst<Mmap>, state: Backtrack<'a>) -> Outcome<'a> {
        match state {
            Some((node, output, index)) => {
                let t = node.transition(index);
                let output = output.cat(t.out);
                let next = raw.node(t.addr);
                Outcome::ChooseBiggest(ChooseBiggest { node: next, output })
            }
            None => Outcome::Abort,
        }
    }
    #[inline]
    fn choose_biggest<'a>(raw: &'a Fst<Mmap>, state: ChooseBiggest<'a>) -> Outcome<'a> {
        if state.node.len() == 0 {
            return Outcome::Final(state.output.cat(state.node.final_output()));
        }
        let t = state.node.transition(state.node.len() - 1);
        let output = state.output.cat(t.out);
        let next = raw.node(t.addr);
        return Outcome::ChooseBiggest(ChooseBiggest { node: next, output });
    }
    #[inline]
    fn compare_next<'a>(raw: &'a Fst<Mmap>, state: CompareNext<'a>) -> Outcome<'a> {
        let input = match state.input.first() {
            Some(&input) => input,
            None => return Outcome::Final(state.output.cat(state.node.final_output())),
        };

        match state.node.find_input(input) {
            None => {
                if state.node.len() == 0 {
                    return Outcome::Abort;
                }
                let mut it = state.node.transitions().enumerate();
                let index = loop {
                    if let Some((index, t)) = it.next() {
                        if t.inp > input {
                            break index;
                        }
                    } else {
                        break state.node.len();
                    }
                };

                if index == 0 {
                    // none is greater than b, either we are equal to t(0), which would have caused find_input to work,
                    // or we are lower than t(0), in which case we should backtrace to the previous byte
                    return Outcome::Backtrack(state.backtrack);
                } else {
                    let t = state.node.transition(index - 1);
                    let output = state.output.cat(t.out);
                    let next = raw.node(t.addr);
                    return Outcome::ChooseBiggest(ChooseBiggest { node: next, output });
                }
            }
            Some(index) => {
                let backtrack = if index == 0 {
                    state.backtrack
                } else {
                    Some((state.node, state.output, index - 1))
                };
                let t = state.node.transition(index);
                let output = state.output.cat(t.out);
                let next = raw.node(t.addr);
                return Outcome::CompareNext(CompareNext {
                    node: next,
                    output,
                    backtrack,
                    input: &state.input[1..],
                });
            }
        }
    }
    #[inline]
    pub fn get_less_or_equal(&self, key: &[u8]) -> Option<&[u8]> {
        let raw = self.fst_map.as_fst();

        let mut outcome = Outcome::CompareNext(CompareNext {
            node: raw.root(),
            output: Output::zero(),
            backtrack: None,
            input: &key,
        });
        loop {
            outcome = match outcome {
                Outcome::CompareNext(state) => Self::compare_next(raw, state),
                Outcome::ChooseBiggest(state) => Self::choose_biggest(raw, state),
                Outcome::Backtrack(state) => Self::backtrack(raw, state),
                Outcome::Final(output) => {
                    let (start, end) = unpack(output.value());
                    return self.items.get_bytes(start as usize, (end - start) as usize)
                }
                Outcome::Abort => return None,
            }
        }
    }
    #[inline]
    pub fn get_less_or_equal_v1(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(output) = self.get_le(key) {
            let (start, end) = unpack(output.value());
            return self.items.get_bytes(start as usize, (end - start) as usize)
        }
        else {
            return None
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
}