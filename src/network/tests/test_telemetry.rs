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
use ton_api::ton::rpc::ton_node::PrepareBlockProof;

#[test]
pub fn test_fullnode_service_telemetry() {
    let t = FullNodeNetworkTelemetry::new(FullNodeNetworkTelemetryKind::Client);
    for i in 1..1000 {
        t.consumed_query(
            format!("{}", std::any::type_name::<PrepareBlockProof>()),
            i % 5 != 0, Duration::from_millis(1 + i), (1024 + i) as usize);
    }
    let r = t.report(5);
    println!("{}", r);
    assert_eq!(r, 
"                                   query       total       bytes  succeded   time: min   avg   max    failed    time: min   avg   max
↓ton_api..c::ton_node::PrepareBlockProof         999     1522476       800( 80%)     2   501  1000       199( 20%)      6   501   996
Total: 1522476 bytes, 304495 bytes/secons");

    for i in 1..100 {
        t.consumed_query("query number two".to_owned(), i % 7 != 0, Duration::from_millis(1 + i), (1024 * 1024 + i) as usize);
    }
    let r = t.report(5);
    println!("{}", r);
     assert_eq!(r, 
"                                   query       total       bytes  succeded   time: min   avg   max    failed    time: min   avg   max
↓                       query number two          99   103813974        85( 86%)     2    50   100        14( 14%)      8    53    99
Total: 103813974 bytes, 20762794 bytes/secons");

    for i in 1..1024 {
        t.consumed_query("query number three".to_owned(), false, Duration::from_millis(1 + i), (1024 * i) as usize);
    }
    let r = t.report(5);
    println!("{}", r);
    assert_eq!(r, 
"                                   query       total       bytes  succeded   time: min   avg   max    failed    time: min   avg   max
↓                     query number three        1023   536346624         0                              1023(100%)      2   513  1024
Total: 536346624 bytes, 107269324 bytes/secons");

    for i in 1..24 {
        t.consumed_query("query number four".to_owned(), true, Duration::from_millis(1 + i), 0);
    }
    let r = t.report(5);
    println!("{}", r);
    assert_eq!(r, 
"                                   query       total       bytes  succeded   time: min   avg   max    failed    time: min   avg   max
↓                      query number four          23           0        23(100%)     2    13    24         0
Total: 0 bytes, 0 bytes/secons");
}