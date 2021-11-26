/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

// We need big size buffer for good rand bytes

#![allow(dead_code)]

use openssl::rand::rand_bytes;


/*
pub struct Randbuf {
    buf: Vec<u8>,
    buf_size: usize
}

impl Randbuf {
    pub fn init (value: usize) -> Self {
        Self {
            buf: vec![0 as u8; 0],
            buf_size: value
        }
    }

    fn gen(&mut self) {
        rand_bytes(&mut self.buf).unwrap();
    }

    pub fn secure_bytes (&mut self, mut orig: &mut Vec<u8>, mut size: usize){
        if self.buf.len() == 0 {
            self.buf.resize(self.buf_size, 0 as u8);
            self.gen();
        }
        if orig.len() != size {
            orig.resize(size, 0 as u8);
        }
        let ready = std::cmp::min(size, self.buf.len());   
        if size > self.buf_size {
            rand_bytes(&mut orig).unwrap();
            return;
        }
        if ready != 0 {
            orig[..ready - 1].copy_from_slice(&self.buf[self.buf.len() - ready..self.buf.len() - 1]);
            size -= ready;
            self.buf.resize(size, 0 as u8);
            if size == 0 {
                return;
            }
        }
        self.buf.resize(self.buf_size, 0 as u8);
        self.gen();
        orig[ready..].copy_from_slice(&self.buf[self.buf.len() - size..self.buf.len() - 1]);
        return;
    }
}
*/

pub fn secure_bytes(mut orig: &mut Vec<u8>, size: usize) {
    orig.resize(size, 0 as u8);
    rand_bytes(&mut orig).unwrap();
}

pub fn secure_256_bits() -> [u8; 32] {
    let mut buf = [0; 32];
    rand_bytes(&mut buf).unwrap();
    buf
}
