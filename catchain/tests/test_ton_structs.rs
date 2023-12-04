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

use catchain::*;

#[test]
fn test_block_update() {
    let block_update = ton::BlockUpdateEvent::default();
    pretty_assertions::assert_eq!(
        format!(
            "{:?}", block_update), 
            "BlockUpdate { block: Block { incarnation: 0000000000000000000000000000000000000000000000000000000000000000, \
            src: 0, height: 0, data: Data { prev: Dep { src: 0, height: 0, \
            data_hash: 0000000000000000000000000000000000000000000000000000000000000000, signature: <> }, \
            deps: Vector([]) }, signature: <> } }"
    );
}
