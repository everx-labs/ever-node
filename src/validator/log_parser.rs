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

#![allow(dead_code)]

extern crate regex;
use std::str::FromStr;
use self::regex::{Regex, escape};

pub struct LogParser {
    log: String
}

impl LogParser {
    pub fn new (s: &str) -> Self {
        let mut prepared_str = " ".to_string();
        prepared_str.push_str(s);
        prepared_str.push_str(" ");
        LogParser { log: prepared_str }
    }

    pub fn get_field (&self, name: &str) -> Option <String> {
        let it = Regex::new (&(format! (" {} = ([^ ]*) ", escape(name)))).unwrap();
        match it.captures (&self.log) {
            None => None,
            Some (group) => Some (group.get(1).unwrap().as_str().to_string())
        }
    }

    pub fn parse_field_fromstr <T> (&self, name: &str) -> T
    where T: FromStr, T::Err: std::fmt::Debug
    {
        match self.get_field (name) {
            None => panic!("Cannot find field `{}`", name),
            Some (v) => T::from_str(&v).unwrap()
        }
    }

    pub fn get_field_count (&self, name: &str) -> u32 {
        let it = Regex::new (&(format! (r" {}\.(\d+)[. ]", escape(name)))).unwrap();
        let mut fields = 0;
        for nums in it.captures_iter (&self.log) {
            let fnew = u32::from_str(&nums[1]).unwrap();
            if fnew >= fields {
                fields = fnew + 1;
            }
        }
        fields
    }

    pub fn parse_slice (&self, name: &str) -> ::ton_api::ton::bytes {
        let data = self.get_field (name).unwrap();
        return ::ton_api::ton::bytes (hex::decode (data).unwrap());
    }
}

