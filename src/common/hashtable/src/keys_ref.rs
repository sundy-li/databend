// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::Hasher;

use ahash::AHasher;

use super::HashTableKeyable;

#[derive(Clone, Copy)]
pub struct KeysRef {
    pub length: usize,
    pub address: usize,
}

impl KeysRef {
    pub fn create(address: usize, length: usize) -> KeysRef {
        KeysRef { length, address }
    }

    #[inline]
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.address as *const u8, self.length)
    }
}

impl Eq for KeysRef {}

impl PartialEq for KeysRef {
    fn eq(&self, other: &Self) -> bool {
        if self.length != other.length {
            return false;
        }

        unsafe {
            let self_value = self.as_slice();
            let other_value = other.as_slice();
            self_value == other_value
        }
    }
}

impl HashTableKeyable for KeysRef {
    const BEFORE_EQ_HASH: bool = true;

    fn is_zero(&self) -> bool {
        self.length == 0
    }

    fn fast_hash(&self) -> u64 {
        unsafe {
            // TODO(Winter) We need more efficient hash algorithm
            let value = self.as_slice();
            let mut hasher = AHasher::default();
            hasher.write(value);
            hasher.finish()
        }
    }

    fn set_key(&mut self, new_value: &Self) {
        self.length = new_value.length;
        self.address = new_value.address;
    }
}

impl std::hash::Hash for KeysRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let self_value =
            unsafe { std::slice::from_raw_parts(self.address as *const u8, self.length) };
        self_value.hash(state);
    }
}
