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

#[tokio::test]
async fn test_read_package() -> Result<()> {
    let data = tokio::fs::read("src/tests/static/archive.1459995.pack").await?;
    let maps = read_package(&data).await?;

    assert_eq!(maps.mc_blocks_ids.len(), 5);
    assert_eq!(maps.blocks.len(), 155);

    for (id, entry) in maps.blocks.iter() {
        if id.is_masterchain() {
            assert!(maps.mc_blocks_ids.contains_key(&id.seq_no()));
        }
        assert!(entry.block.is_some());
        assert!(entry.proof.is_some());
    }
    Ok(())
}

