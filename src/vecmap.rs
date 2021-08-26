use memmap::{MmapMut, MmapOptions};
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::ops::{Deref, DerefMut};
use std::{cmp::max, mem::size_of,};
pub struct VecMap {
    pub mmap: MmapMut,
    pub file: File
}


impl VecMap {
    pub fn with_capacity(fp: &str, cap: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).append(true).create(true).open(fp)?;
        let size = max(1, cap);
        file.set_len(size as u64);
        let mut mmap = unsafe {MmapOptions::new().map_mut(&file)?};
        let size_bytes = u32::to_le_bytes(4);
        (&mut mmap[..4]).write(&size_bytes)?;
        Ok(Self {
            mmap,
            file
        })
    }
    pub fn from_file(fp: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).append(true).create(true).open(fp)?;
        let mmap = unsafe {MmapMut::map_mut(&file)?};
        Ok(Self {
            mmap,
            file
        })
    }
    #[inline]
    pub fn len(&self) -> usize {
        u32::from_le_bytes(*pop(&self.mmap[..4])) as usize
    }
    #[inline]
    pub fn set_len(&mut self, len: usize) {
        let size_bytes = u32::to_le_bytes(len as u32);
        (&mut self.mmap[..4]).write(&size_bytes);
    }
    #[inline]
    pub fn get_bytes(&self, index: usize, len: usize) -> Option<&[u8]> {
        if let Some(slice) = self.mmap.get(index..) {
            let res=slice.get(0..len);
            res           
        }
        else {None}
    }
    pub fn push(&mut self, elem: &[u8]) -> std::io::Result<u64> {
        let start = self.len();   
        let end = start + elem.len();
        if end > self.mmap.len() {
            self.file.set_len(end as u64)?;
            self.mmap = unsafe {MmapOptions::new().map_mut(&self.file)?};
        }
        else {}
        (&mut self.mmap[start..end]).write(elem)?;
        self.set_len(end);
        let packed = pack(start as u32, end as u32);
        self.mmap.flush_async()?;
        Ok(packed)
    }
}
#[inline]
fn pop(barry: &[u8]) -> &[u8; 4] {
    barry.try_into().expect("slice with incorrect length")
}
#[inline]
pub fn pack(x: u32, y: u32) -> u64 {
    (x as u64) << 32 | (y as u64)
}
#[inline]
pub fn unpack(input: u64) -> (u32, u32) {
    let x = ((input & 0xFFFFFFFF00000000) >> 32) as u32;
    let y = (input & 0xFFFFFFFF) as u32;
    (x, y)
}