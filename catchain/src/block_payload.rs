pub use super::*;
use std::fmt;

/*
    Implementation details for BlockPayload
*/

pub(crate) struct BlockPayloadImpl {
    data: RawBuffer,           //raw data
    creation_time: SystemTime, //time of block creation
}

/*
    Implementation for public BlockPayload trait
*/

impl BlockPayload for BlockPayloadImpl {
    fn data(&self) -> &RawBuffer {
        &self.data
    }

    fn get_creation_time(&self) -> std::time::SystemTime {
        self.creation_time
    }
}

/*
    Implementation for public Debug trait
*/

impl fmt::Debug for BlockPayloadImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

/*
    Implementation of BlockPayloadImpl
*/

impl BlockPayloadImpl {
    pub(crate) fn create(data: RawBuffer) -> BlockPayloadPtr {
        Arc::new(Self {
            data: data,
            creation_time: SystemTime::now(),
        })
    }
}
