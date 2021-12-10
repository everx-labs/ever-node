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

use crate::external_db::WriteData;

use ton_types::Result;

#[derive(Clone)]
pub struct StubProducer {
    pub enabled: bool
}

#[async_trait::async_trait]
impl WriteData for StubProducer {
    fn enabled(&self) -> bool { self.enabled }
    fn sharding_depth(&self) -> u32 { 0 }
    async fn write_data(&self, _key: String, _data: String, _attributes: Option<&[(&str, &[u8])]>, _partition_key: Option<u32>) -> Result<()> {
        futures_timer::Delay::new(std::time::Duration::from_millis(3)).await;
        Ok(())
    }
    async fn write_raw_data(&self, _key: Vec<u8>, _data: Vec<u8>, _attributes: Option<&[(&str, &[u8])]>, _partition_key: Option<u32>) -> Result<()> {
        futures_timer::Delay::new(std::time::Duration::from_millis(3)).await;
        Ok(())
    }
}
