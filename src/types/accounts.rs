use std::{sync::{Arc, atomic::AtomicU64}};
use ton_block::{
    Serializable, ShardAccount, ShardAccounts,
    AccountBlock, Transaction, Transactions, HashUpdate, LibDescr,
    Augmentation, HashmapAugType, Libraries, StateInitLib
};
use ton_types::{Result, AccountId, Cell, HashmapRemover, fail, UInt256};

pub struct ShardAccountStuff {
    account_addr: AccountId,
    shard_acc: ShardAccount,
    lt: Arc<AtomicU64>,
    transactions: Transactions,
    state_update: HashUpdate,
    orig_libs: StateInitLib,
}

impl ShardAccountStuff {
    pub fn from_shard_state(account_addr: AccountId, accounts: &ShardAccounts, block_lt: u64) -> Result<Self> {
        let shard_acc = accounts.account(&account_addr)?.unwrap_or_default();
        let account_hash = shard_acc.account_cell().repr_hash();
        Ok(Self{
            account_addr,
            orig_libs: shard_acc.read_account()?.libraries(),
            shard_acc,
            lt: Arc::new(AtomicU64::new(block_lt)),
            transactions: Default::default(),
            state_update: HashUpdate::with_hashes(account_hash.clone(), account_hash),
        })
    }
    pub fn update_shard_state(&self, new_accounts: &mut ShardAccounts) -> Result<AccountBlock> {
        let account = self.shard_acc.read_account()?;
        if account.is_none() {
            new_accounts.remove(self.account_addr().clone())?;
        } else {
            let value = self.shard_acc.serialize()?;
            new_accounts.set_serialized(self.account_addr().clone(), &value.into(), &account.aug()?)?;
        }
        AccountBlock::with_params(&self.account_addr, &self.transactions, &self.state_update)
    }
    pub fn lt(&self) -> Arc<AtomicU64> {
        self.lt.clone()
    }
    pub fn account_cell(&self) -> &Cell {
        self.shard_acc.account_cell()
    }
    pub fn shard_account(&self) -> &ShardAccount {
        &self.shard_acc
    }
    pub fn account_addr(&self) -> &AccountId {
        &self.account_addr
    }
    pub fn add_transaction(&mut self, transaction: &mut Transaction, account_root: Cell) -> Result<()> {
        transaction.set_prev_trans_hash(self.shard_acc.last_trans_hash().clone());
        transaction.set_prev_trans_lt(self.shard_acc.last_trans_lt());
        // log::trace!("{} {}", self.collated_block_descr, debug_transaction(transaction.clone())?);

        self.state_update.new_hash = account_root.repr_hash();

        let tr_root = transaction.serialize()?;
        self.shard_acc.set_account_cell(account_root);
        self.shard_acc.set_last_trans_hash(tr_root.repr_hash());
        self.shard_acc.set_last_trans_lt(transaction.logical_time());

        self.transactions.setref(
            &transaction.logical_time(),
            &tr_root,
            transaction.total_fees()
        )?;

        Ok(())
    }
    pub fn serialize(&self) -> Result<Cell> {
        self.shard_acc.serialize()
    }
    pub fn update_public_libraries(&self, libraries: &mut Libraries) -> Result<()> {
        let new_libs = self.shard_acc.read_account()?.libraries();
        if new_libs.root() != self.orig_libs.root() {
            new_libs.scan_diff(&self.orig_libs, |key: UInt256, old, new| {
                let old = old.unwrap_or_default();
                let new = new.unwrap_or_default();
                if old.is_public_library() && !new.is_public_library() {
                    self.remove_public_library(key, libraries)?;
                } else if !old.is_public_library() && new.is_public_library() {
                    self.add_public_library(key, new.root, libraries)?;
                }
                Ok(true)
            })?;
        }
        Ok(())
    }
    pub fn remove_public_library(&self, key: UInt256, libraries: &mut Libraries) -> Result<()> {
        log::trace!("Removing public library {:x} of account {:x}", key, self.account_addr);

        let mut lib_descr = match libraries.get(&key)? {
            Some(ld) => ld,
            None => fail!("cannot remove public library {:x} of account {} because this public \
                library did not exist", key, self.account_addr)
        };

        if lib_descr.lib().repr_hash() != key {
            fail!(
                "cannot remove public library {:x} of account {:x} because this public library \
                LibDescr record does not contain a library root cell with required hash", key, self.account_addr);
        }

        if !lib_descr.publishers_mut().remove(&self.account_addr)? {
            fail!("cannot remove public library {:x} of account {:x} because this public library \
                LibDescr record does not list this account as one of publishers", key, self.account_addr);
        }

        if lib_descr.publishers().is_empty() {
            log::debug!("library {:x} has no publishers left, removing altogether", key);
            libraries.remove(&key)?;
        } else {
            libraries.set(&key, &lib_descr)?;
        }

        return Ok(())
    }
    pub fn add_public_library(
        &self, 
        key: UInt256,
        library: Cell,
        libraries: &mut Libraries
    ) -> Result<()> {
        log::trace!("Adding public library {:x} of account {:x}", key, self.account_addr);
        
        if key != library.repr_hash() {
            fail!("Can't add library {:x} because it mismatch given key");
        }

        let lib_descr = if let Some(mut old_lib_descr) = libraries.get(&key)? {
            if old_lib_descr.lib().repr_hash() != library.repr_hash() {
                fail!("cannot add public library {:x} of account {:x} because existing LibDescr \
                    record for this library does not contain a library root cell with required hash",
                    key, self.account_addr);
            }
            if old_lib_descr.publishers().check_key(&self.account_addr)? {
                fail!("cannot add public library {:x} of account {:x} because this public library's \
                    LibDescr record already lists this account as a publisher",
                    key, self.account_addr);
            }
            old_lib_descr.publishers_mut().set(&self.account_addr, &())?;
            old_lib_descr
        } else {
            LibDescr::from_lib_data_by_publisher(library, self.account_addr.clone())
        };

        libraries.set(&key, &lib_descr)?;

        return Ok(());
      }
}
