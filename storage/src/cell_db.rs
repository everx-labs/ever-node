use crate::{
    db_impl_base, db::traits::{KvcTransaction, KvcTransactional},
    dynamic_boc_db::DynamicBocDb, types::{CellId, Reference, StorageCell}
};
use std::{io::{Cursor, Write}, sync::Arc};
use ton_types::{ByteOrderRead, Cell, CellData, Result, MAX_REFERENCES_COUNT, UInt256};

db_impl_base!(CellDb, KvcTransactional, CellId);

impl CellDb {
    /// Gets cell from key-value storage by cell id
    pub fn get_cell(&self, cell_id: &CellId, boc_db: Arc<DynamicBocDb>) -> Result<StorageCell> {
        let (cell_data, references) = Self::deserialize_cell(self.db.get(&cell_id)?.as_ref())?;
        Ok(StorageCell::with_params(cell_data, references, boc_db))
    }

    /// Puts cell into transaction
    pub fn put_cell<T: KvcTransaction<CellId> + ?Sized>(transaction: &T, cell_id: &CellId, cell: Cell) -> Result<()> {
        transaction.put(cell_id, &Self::serialize_cell(cell)?);
        Ok(())
    }

    /// Binary serialization of cell data
    fn serialize_cell(cell: Cell) -> Result<Vec<u8>> {
        let references_count = cell.references_count() as u8;

        assert!(references_count as usize <= MAX_REFERENCES_COUNT);

        let mut data = Vec::new();

        cell.cell_data().serialize(&mut data)?;
        data.write(&[references_count])?;

        for i in 0..references_count {
            data.write(cell.reference(i as usize)?.repr_hash().as_slice())?;
        }

        assert!(!data.is_empty());

        Ok(data)
    }

    /// Binary deserialization of cell data
    pub(crate) fn deserialize_cell(data: &[u8]) -> Result<(CellData, Vec<Reference>)> {
        assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = reader.read_byte()?;
        let mut references = Vec::with_capacity(references_count as usize);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }

        Ok((cell_data, references))
    }
}
