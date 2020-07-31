use crate::external_db::WriteData;

use ton_types::Result;

#[derive(Clone)]
pub struct StubProducer {
    pub enabled: bool
}

#[async_trait::async_trait]
impl WriteData for StubProducer {
    fn enabled(&self) -> bool { self.enabled }
    async fn write_data(&self, _key: String, _data: String) -> Result<()> {
        futures_timer::Delay::new(std::time::Duration::from_millis(3)).await;
        Ok(())
    }
    async fn write_raw_data(&self, _key: Vec<u8>, _data: Vec<u8>) -> Result<()> {
        futures_timer::Delay::new(std::time::Duration::from_millis(3)).await;
        Ok(())
    }
}