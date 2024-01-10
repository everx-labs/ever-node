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

use crate::rng::random::secure_bytes;

#[test]
fn test_gen_rand() {
    let mut orig: Vec<u8> = Vec::new();
    secure_bytes(&mut orig, 112);
    assert!(orig.len() == 112);
    println!("{:?}", &orig);
    secure_bytes(&mut orig, 630);
    assert!(orig.len() == 630);
    println!("{:?}", &orig);
    secure_bytes(&mut orig, 490);
    assert!(orig.len() == 490);
    println!("{:?}", &orig);
    secure_bytes(&mut orig, 110);
    assert!(orig.len() == 110);   
    println!("{:?}", &orig); 
}

