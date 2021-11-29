
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LtDesc {
    first_index: u32,
    last_index: u32,
    last_seq_no: u32,
    last_lt: u64,
    last_unix_time: u32,
}

impl LtDesc {
    pub const fn with_values(first_index: u32, last_index: u32, last_seq_no: u32, last_lt: u64, last_unix_time: u32) -> Self {
        Self { first_index, last_index, last_seq_no, last_lt, last_unix_time }
    }

    pub const fn first_index(&self) -> u32 {
        self.first_index
    }

/*
    pub fn set_first_index(&mut self, value: u32) {
        self.first_index = value
    }
*/

    pub const fn last_index(&self) -> u32 {
        self.last_index
    }

/*
    pub fn set_last_index(&mut self, value: u32) {
        self.last_index = value
    }
*/

    pub const fn last_seq_no(&self) -> u32 {
        self.last_seq_no
    }

/*
    pub fn set_last_seq_no(&mut self, value: u32) {
        self.last_seq_no = value;
    }
*/

    pub const fn last_lt(&self) -> u64 {
        self.last_lt
    }

/*
    pub fn set_last_lt(&mut self, value: u64) {
        self.last_lt = value;
    }
*/

    pub const fn last_unix_time(&self) -> u32 {
        self.last_unix_time
    }

/*
    pub fn set_last_unix_time(&mut self, value: u32) {
        self.last_unix_time = value;
    }
*/

}
