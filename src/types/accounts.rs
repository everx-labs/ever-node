/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

use ton_block::{
    Account, AccountBlock, Augmentation, CopyleftRewards, Deserializable, HashUpdate,
    HashmapAugType, LibDescr, Libraries, Serializable, ShardAccount, ShardAccounts, StateInitLib,
    Transaction, Transactions,
};
use ton_types::{error, fail, AccountId, Cell, HashmapRemover, Result, UInt256, SliceData};

pub struct ShardAccountStuff {
    account_addr: AccountId,
    account_root: Cell,
    last_trans_hash: UInt256,
    last_trans_lt: u64,
    lt: u64,
    transactions: Option<Transactions>,
    state_update: HashUpdate,
    orig_libs: Option<StateInitLib>,
    copyleft_rewards: Option<CopyleftRewards>,

    /// * Sync key of message, which updated account state
    /// * It is an incremental counter set by executor
    update_msg_sync_key: Option<usize>,

    // /// * Executor sets transaction that updated account to current state
    // /// * Initial account state contains None
    // last_transaction: Option<(Cell, CurrencyCollection)>,

    /// LT of transaction, which updated account state
    update_trans_lt: Option<u64>,

    /// The copyleft_reward of transaction, which updated account state (if exists)
    update_copyleft_reward_address: Option<AccountId>,

    /// Executor stores prevoius account state
    prev_account_stuff: Option<Box<ShardAccountStuff>>,
}

impl ShardAccountStuff {
    pub fn new(
        account_addr: AccountId,
        shard_acc: ShardAccount,
        lt: u64,
    ) -> Result<Self> {
        let account_hash = shard_acc.account_cell().repr_hash();
        let account_root = shard_acc.account_cell();
        let last_trans_hash = shard_acc.last_trans_hash().clone();
        let last_trans_lt = shard_acc.last_trans_lt();
        Ok(Self{
            account_addr,
            orig_libs: Some(shard_acc.read_account()?.libraries()),
            account_root,
            last_trans_hash,
            last_trans_lt,
            lt,
            transactions: Some(Transactions::default()),
            state_update: HashUpdate::with_hashes(account_hash.clone(), account_hash),
            copyleft_rewards: Some(CopyleftRewards::default()),

            update_msg_sync_key: None,
            //last_transaction: None,
            update_trans_lt: None,
            update_copyleft_reward_address: None,
            prev_account_stuff: None,
        })
    }
    /// Returns:
    /// * None - if no updates or no matching records in history
    /// * Some(particular) - record from history that matches update_msg_sync_key == on_msg_sync_key
    pub fn commit(mut self, on_msg_sync_key: usize) -> Result<Option<Self>> {
        while let Some(current_update_msg_sync_key) = self.update_msg_sync_key {
            if current_update_msg_sync_key == on_msg_sync_key {
                log::debug!("account {:x} state committed by processed message {} in the queue", self.account_addr(), on_msg_sync_key);
                return Ok(Some(self));
            } else {
                if !self.revert()? {
                    log::debug!("unable to revert account {:x} state, current state is a first in history", self.account_addr());
                    return Ok(None);
                } else {
                    log::debug!("account {:x} state reverted one step back to message {:?} in the queue", self.account_addr(), self.update_msg_sync_key);
                }
            }
        }
        Ok(None)
    }
    fn revert(&mut self) -> Result<bool> {
        let mut taked_prev = match self.prev_account_stuff.take() {
            Some(prev) => prev,
            None => return Ok(false),
        };
        let prev = taked_prev.as_mut();

        prev.orig_libs = self.orig_libs.take();

        prev.transactions = self.transactions.take();
        if let Some(update_trans_lt) = self.update_trans_lt {
            prev.remove_trans(update_trans_lt)?;
        }

        prev.copyleft_rewards = self.copyleft_rewards.take();
        if let Some(update_copyleft_reward_address) = self.update_copyleft_reward_address.as_ref() {
            prev.remove_copyleft_reward(update_copyleft_reward_address)?;
        }

        std::mem::swap(self, prev);

        Ok(true)
    }
    pub fn update_shard_state(&mut self, new_accounts: &mut ShardAccounts) -> Result<AccountBlock> {
        let account = self.read_account()?;
        if account.is_none() {
            new_accounts.remove(self.account_addr().clone())?;
        } else {
            let shard_acc = ShardAccount::with_account_root(self.account_root(), self.last_trans_hash.clone(), self.last_trans_lt);
            let value = shard_acc.write_to_new_cell()?;
            new_accounts.set_builder_serialized(self.account_addr().clone(), &value, &account.aug()?)?;
        }
        AccountBlock::with_params(&self.account_addr, self.transactions()?, &self.state_update)
    }
    pub fn lt(&self) -> u64 {
        self.lt
    }
    pub fn read_account(&self) -> Result<Account> {
        Account::construct_from_cell(self.account_root())
    }
    pub fn account_root(&self) -> Cell {
        self.account_root.clone()
    }
    pub fn last_trans_lt(&self) -> u64 {
        self.last_trans_lt
    }
    pub fn account_addr(&self) -> &AccountId {
        &self.account_addr
    }
    pub fn copyleft_rewards(&self) -> Result<&CopyleftRewards> {
        self.copyleft_rewards.as_ref()
            .ok_or_else(|| error!(
                "`copyleft_rewards` field is None, possibly you try access a not root record in the history, run commit() before"
            ))
    }
    fn copyleft_rewards_mut(&mut self) -> Result<&mut CopyleftRewards> {
        self.copyleft_rewards.as_mut()
            .ok_or_else(|| error!(
                "`copyleft_rewards` field is None, possibly you try access a not root record in the history, run commit() before"
            ))
    }
    fn remove_copyleft_reward(&mut self, address: &AccountId) -> Result<bool> {
        self.copyleft_rewards_mut()?.remove(address)
    }

    fn transactions(&self) -> Result<&Transactions> {
        self.transactions.as_ref()
            .ok_or_else(|| error!(
                "`transactions` field is None, possibly you try access a not root record in the history, run commit() before"
            ))
    }
    fn transactions_mut(&mut self) -> Result<&mut Transactions> {
        self.transactions.as_mut()
            .ok_or_else(|| error!(
                "`transactions` field is None, possibly you try access a not root record in the history, run commit() before"
            ))
    }
    fn remove_trans(&mut self, trans_lt: u64) -> Result<()> {
        let key = SliceData::load_builder(trans_lt.write_to_new_cell()?)?;
        self.transactions_mut()?.remove(key)?;
        Ok(())
    }

    fn orig_libs(&self) -> Result<&StateInitLib> {
        self.orig_libs.as_ref()
            .ok_or_else(|| error!(
                "`orig_libs` field is None, possibly you try access a not root record in the history, run commit() before"
            ))
    }

    pub fn apply_transaction_res(
        &mut self,
        update_msg_sync_key: usize,
        tx_last_lt: u64,
        transaction_res: &mut Result<Transaction>,
        account_root: Cell,
    ) -> Result<()> {
        let mut res = ShardAccountStuff {
            account_addr: self.account_addr.clone(),
            account_root: self.account_root.clone(),
            last_trans_hash: self.last_trans_hash.clone(),
            last_trans_lt: self.last_trans_lt,
            lt: tx_last_lt, // 1014 or 1104 or 1024
            transactions: self.transactions.take(),
            state_update: self.state_update.clone(),
            orig_libs: self.orig_libs.take(),
            copyleft_rewards: Some(CopyleftRewards::default()),
            update_msg_sync_key: Some(update_msg_sync_key),
            update_trans_lt: None,
            update_copyleft_reward_address: None,
            prev_account_stuff: None,
        };

        if let Ok(transaction) = transaction_res {
            res.add_transaction(transaction, account_root)?;
        }

        std::mem::swap(self, &mut res);

        self.prev_account_stuff = Some(Box::new(res));

        Ok(())
    }
    pub fn add_transaction(&mut self, transaction: &mut Transaction, account_root: Cell) -> Result<()> {
        transaction.set_prev_trans_hash(self.last_trans_hash.clone());
        transaction.set_prev_trans_lt(self.last_trans_lt); // 1010
        // log::trace!("{} {}", self.collated_block_descr, debug_transaction(transaction.clone())?);

        self.account_root = account_root;
        self.state_update.new_hash = self.account_root.repr_hash();

        let tr_root = transaction.serialize()?;
        let tr_lt = transaction.logical_time(); // 1011

        self.last_trans_hash = tr_root.repr_hash();
        self.last_trans_lt = tr_lt;

        self.update_trans_lt = Some(tr_lt);

        self.transactions_mut()?.setref(
            &tr_lt,
            &tr_root,
            transaction.total_fees()
        )?;

        if let Some(copyleft_reward) = transaction.copyleft_reward() {
            log::trace!("Copyleft reward {} {} from transaction {}", copyleft_reward.address, copyleft_reward.reward, self.last_trans_hash);
            self.copyleft_rewards_mut()?.add_copyleft_reward(&copyleft_reward.address, &copyleft_reward.reward)?;
            self.update_copyleft_reward_address = Some(copyleft_reward.address.clone());
        }

        Ok(())
    }
    pub fn update_public_libraries(&self, libraries: &mut Libraries) -> Result<()> {
        let account = self.read_account()?;
        let new_libs = account.libraries();
        let orig_libs = self.orig_libs()?;
        if new_libs.root() != orig_libs.root() {
            new_libs.scan_diff(orig_libs, |key: UInt256, old, new| {
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
