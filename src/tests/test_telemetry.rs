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

use super::*;
use std::time::Duration;
use ton_block::ShardIdent;

#[test]
pub fn test_fullnode_telemetry() {
    let t = FullNodeTelemetry::new();
    for i in 1..1000 {
        let id = BlockIdExt::with_params(
            ShardIdent::masterchain(),
            i,
            Default::default(),
            Default::default()
        );
        t.good_top_block_broadcast(&id);
        if i % 21 != 0 {
            t.good_top_block_broadcast(&id);
            t.good_top_block_broadcast(&id);
        } else {
            t.bad_top_block_broadcast();
        }
        if i % 5 != 0 {
            std::thread::sleep(Duration::from_millis((1 + i % 3) as u64));
            t.new_block_broadcast(&id, i % 11 == 0, i % 13 == 0);
        }
        if i % 4 == 0 {
            for _ in 0..((i % 48) / 4) + 1 {
                t.new_downloading_block_attempt(&id);
                std::thread::sleep(Duration::from_millis(1));
            }
            t.new_downloaded_block(&id);
        }
        t.new_pre_applied_block(i % 5 != 0);
        if i % 31 != 0 { 
            t.new_applied_block();
        }
        t.sent_top_block_broadcast();
        t.sent_block_broadcast();
        t.sent_ext_msg_broadcast();
    }
    let r = t.report(0, 0);
    print!("{}", r);
    assert!(r.starts_with(
"applied blocks                     967  97%
pre-applied blocks                 999 100%
    got by broadcast               800  80%
    downloaded                     199  20%
transactions per second
    for 300sec window            0
    for  60sec window            0
***
sent broadcasts:
    top block descriptions         999
    blocks                         999
    external messages              999
***
bad top block broadcast             47
good top block broadcast           999 100%
top block broadcast dupl          1904 191%
*
lost block broadcasts                0
block broadcast                    800 100%
block broadcast duplicates          72   9%
unneeded block-broadcast            61   8%
(block was already downloaded)
***
block download attempts:
                         1          20   8%
                         2          21   8%
                         3          21   8%
                         4          21   8%
                         5          21   8%
                         6          21   8%
                         7          21   8%
                         8          21   8%
                         9          21   8%
                        10          21   8%
                      more          40  16%
***
block download time (min avg max)")); // last metrics are different from time to time
}

