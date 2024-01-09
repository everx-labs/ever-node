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

use std::fs;
use super::*;

#[test]
fn test_log_parser() {
    let contents = fs::read_to_string("src/validator/tests/static/test_log_parser.log")
        .expect("File cannot be read/not found")
        .replace("\n"," ");
    println!("Contents: {}", contents);
    let parser = LogParser::new (&contents);

    // Basic value parsing
    assert_eq!(parser.parse_field_fromstr::<u32>("shard.workchain"), 0);
    assert_eq!(parser.parse_field_fromstr::<i32>("min_masterchain_block.id.workchain"), -1);

    // Slice parsing
    let collated_file_hash = parser.parse_slice("candidate.collated_file_hash");
    assert_eq!(collated_file_hash.0[31], 0x55);
    assert_eq!(collated_file_hash.0, hex::decode("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").unwrap());

    // Empty slice parsing
    assert_eq!(parser.parse_slice("candidate.collated_data_snd").0, Vec::<u8>::default());

    // Indexed fields counting
    let validator_count = parser.get_field_count ("validator");
    assert_eq!(validator_count, 5);

    // Indexed fields parsing
    for validator in 0..validator_count {
        let field_name = format!("validator.{}.data.size", validator);
        assert_eq!(parser.parse_field_fromstr::<u32>(&field_name), 32);

        let data_name = format!("validator.{}.data", validator);
        assert_eq!(parser.parse_slice(&data_name).0.len(), 32);
    }

    // Last field
    assert_eq!(parser.parse_field_fromstr::<u32>("validator.4.weight"), 1);
}

#[test]
fn test_log_parser2() {
    let parser = LogParser::new (" .. = 1 *\\.0 = 7 *\\.1 = 14 *\\.11.w = 12 *\\.21a = 77 ., = 2 ** = 3 [[](.,& = 101 ");
    assert_eq!(parser.parse_field_fromstr::<u32>(".."), 1);
    assert_eq!(parser.parse_field_fromstr::<u32>(".,"), 2);
    assert_eq!(parser.parse_field_fromstr::<u32>("**"), 3);
    assert_eq!(parser.parse_field_fromstr::<u32>("[[](.,&"), 101);
    assert_eq!(parser.get_field_count ("*\\"), 12);
}

