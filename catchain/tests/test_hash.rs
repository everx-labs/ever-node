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

use catchain::utils::*;
use catchain::BlockHash;

fn compute_hash_impl(
    incarnation: &str,
    source_public_key_hash: &str,
    payload: &str,
    height: ton_api::ton::int,
) -> BlockHash {
    let incarn = parse_hex_as_int256(incarnation);
    let pk_hash = parse_hex_as_public_key_hash(source_public_key_hash);

    let pld_vec = parse_hex(payload);
    let pld = ton_api::ton::bytes(pld_vec);

    let block_id = get_block_id(&incarn, &pk_hash, height, &pld);
    println!(
        "incarnation = {}, src = {:?}, height = {}, data = {}",
        incarn.to_hex_string(),
        pk_hash,
        height,
        payload
    );
    get_block_id_hash(&block_id)
}

fn compute_hash(incarnation: &str, source_public_key_hash: &str, block: &str) -> BlockHash {
    let block_bytes = parse_hex(block);
    let mut blb: &[u8] = block_bytes.as_ref();
    let update: ton_api::ton::catchain::Update =
        ton_api::Deserializer::new(&mut blb).read_boxed().unwrap();
    let blk = update.only();
    let pld = ton_api::ton::bytes(blb.to_vec());

    compute_hash_impl(
        incarnation,
        source_public_key_hash,
        &bytes_to_string(&pld),
        blk.block.height,
    )
}

#[test]
fn test_payload_hash() {
    for (pl, hc) in &[
        (
            "5d14cd8dfde429e8e14eec57b420ffe9",
            "FA63063B9847E845DDB69E07FCCBB5EBD1EE15AF502A94AB62953E0DC46D907A",
        ),
        (
            "e13aeaf5f1487760e13aeaf5f1487760",
            "1080F719133552CEE67B16DF7F9FAA60DDB2DFBBB4D66F025E04140A893790E5",
        ),
        (
            "1d214b4407298274c1b19ff71c8a52b5",
            "431B2B26FF4CA8F0333A435DE2A30DB942953B0196DBD256593EF1F8391FAD09",
        ),
    ] {
        assert_eq!(
            get_hash(&ton_api::ton::bytes(parse_hex(pl))),
            parse_hex_as_int256(hc)
        );
    }
}

#[test]
fn test_block_id_hash() {
    for (inc, pk, pld, height, hash) in &[
        (
            "e0e1ec9482633112121bfbad9318e6bb4c46bce23c21278723fc91d80d9f5689",
            "9903f50c19644062ff2cfaf5a061d882ddd931628b7f23478d714625924e1eec",
            "3836a4a85b17f97b3836a4a85b17f97b",
            1,
            "c11ca9a694835c3497ede4043e36e6d53a81dad0a108f00a061a785c146321a8",
        ),
        (
            "e0e1ec9482633112121bfbad9318e6bb4c46bce23c21278723fc91d80d9f5689",
            "9832abeda7a931e65cecc070810f626d8d95a4fd1c5fd628f2c4891d9bbaf514",
            "96b516d523ecb7b196b516d523ecb7b1",
            1,
            "8b6c9c047cb2598c4fa3cb2a3f958e6ac616a19b0dc7c4fd7c20b143dcc89151",
        ),
        (
            "e0e1ec9482633112121bfbad9318e6bb4c46bce23c21278723fc91d80d9f5689",
            "04ba3e4c8935badbeaf2c5aab839d9070fa1e074f9391253010df4766e4d7c05",
            "468c9a372b24185187f63d60fd12d0d5",
            3,
            "1ac5862090754cd2cbda91bf11f143a6ab31c9dba8d31ae4ccfc3c056e0f2a38",
        ),
    ] {
        assert_eq!(
            compute_hash_impl(inc, pk, pld, *height),
            parse_hex_as_int256(hash)
        );
    }
}

#[test]
fn test_block_hash() {
    for (inc, pk, blk, hash) in &[
           ("2a3c82246129cf61307839b7f459cceb9967e0fba9959391fa60beaabbec7931", 
            "53ffecacfadbb8dc29aabf9b73dadabb955ce2172e97640db25d742abd6e65e9", 
            "c45867232a3c82246129cf61307839b7f459cceb9967e0fba9959391fa60beaabbec793105000000010000000b000000000000002a3c82246129cf61307839b7f459cceb9967e0fba9959391fa60beaabbec79310000000000000000404dcd598b949a94ad50c748d24ce52e1e97c38feeef05c3b39b20257647782dd28d5642775a5760a2f0a85c47dab62a855817100f966ea4604919d5e9ef339d09000000e43c7c3d0e19e9e1e43c7c3d0e19e9e1", 
            "3c95513d4d2027b0020a2463f9e0069ad3487c90a4d85fb9aabfabb39834b082")
        ]
    {
        assert_eq! (compute_hash (inc, pk, blk), parse_hex_as_int256 (hash));
    }
}
