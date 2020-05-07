use crate::external_db::WriteData;

use ton_types::Result;

#[derive(Clone)]
pub struct StubProducer;

#[async_trait::async_trait]
impl WriteData for StubProducer {
    async fn write_data(&self, _key: String, _data: String) -> Result<()> {
        futures_timer::Delay::new(std::time::Duration::from_millis(3)).await;
        Ok(())
    }
}