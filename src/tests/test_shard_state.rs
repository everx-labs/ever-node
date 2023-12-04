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
use crate::collator_test_bundle::create_engine_allocated;
#[cfg(feature = "telemetry")]
use crate::collator_test_bundle::create_engine_telemetry;
use ton_block::ShardIdent;

#[test]
fn test_shard_state_deserilaize() {
    let mut ss = ShardStateUnsplit::with_ident(ShardIdent::masterchain());
    let mut zero_state_id = BlockIdExt::with_params(
        ShardIdent::masterchain(),
        0,
        UInt256::default(),
        UInt256::default()
    );
    #[cfg(feature = "telemetry")]
    let telemetry = create_engine_telemetry();
    let allocated = create_engine_allocated();
    let cell = ss.serialize().unwrap();
    let bytes = ton_types::write_boc(&cell).unwrap();
    let check = ShardStateStuff::deserialize_zerostate(
        zero_state_id.clone(), 
        &bytes,
        #[cfg(feature = "telemetry")]
        &telemetry,
        &allocated
    );
    if check.is_ok() {
        panic!("zero state must be checked")
    }
    zero_state_id.root_hash = cell.repr_hash();
    zero_state_id.file_hash = UInt256::calc_file_hash(&bytes);
    let new_ss = ShardStateStuff::deserialize_zerostate(
        zero_state_id.clone(), 
        &bytes,
        #[cfg(feature = "telemetry")]
        &telemetry,
        &allocated
    ).expect("something wrong with zero state checking");
    assert_eq!(new_ss.state().unwrap(), &ss);

    ss.set_seq_no(1);
    let cell = ss.serialize().unwrap();
    let bytes = ton_types::write_boc(&cell).unwrap();
    let block_id = BlockIdExt {
        shard_id: ShardIdent::masterchain(),
        seq_no: 1,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let new_ss = ShardStateStuff::deserialize_state(
        block_id, 
        &bytes,
        #[cfg(feature = "telemetry")]
        &telemetry,
        &allocated
    ).expect("not zero state no need to be checked");
    assert_eq!(new_ss.state().unwrap(), &ss);
}

