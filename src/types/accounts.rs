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

use std::sync::{atomic::AtomicU64, Arc};
use ton_block::{
    Account, AccountBlock, Augmentation, CopyleftRewards, Deserializable, HashUpdate,
    HashmapAugType, LibDescr, Libraries, Serializable, ShardAccount, ShardAccounts, StateInitLib,
    Transaction, Transactions,
};
use ton_types::{fail, AccountId, Cell, HashmapRemover, Result, UInt256};

pub struct ShardAccountStuff {
    account_addr: AccountId,
    account_root: Cell,
    last_trans_hash: UInt256,
    last_trans_lt: u64,
    lt: Arc<AtomicU64>,
    transactions: Transactions,
    state_update: HashUpdate,
    orig_libs: StateInitLib,
    copyleft_rewards: CopyleftRewards,
}

impl ShardAccountStuff {
    pub fn from_shard_state(
        account_addr: AccountId,
        accounts: &ShardAccounts,
        lt: Arc<AtomicU64>,
    ) -> Result<Self> {
        let shard_acc = accounts.account(&account_addr)?.unwrap_or_default();
        let account_hash = shard_acc.account_cell().repr_hash();
        let account_root = shard_acc.account_cell();
        let last_trans_hash = shard_acc.last_trans_hash().clone();
        let last_trans_lt = shard_acc.last_trans_lt();
        Ok(Self{
            account_addr,
            orig_libs: shard_acc.read_account()?.libraries(),
            account_root,
            last_trans_hash,
            last_trans_lt,
            lt,
            transactions: Transactions::default(),
            state_update: HashUpdate::with_hashes(account_hash.clone(), account_hash),
            copyleft_rewards: CopyleftRewards::default(),
        })
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
        AccountBlock::with_params(&self.account_addr, &self.transactions, &self.state_update)
    }
    pub fn lt(&self) -> Arc<AtomicU64> {
        self.lt.clone()
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
    pub fn copyleft_rewards(&self) -> &CopyleftRewards {
        &self.copyleft_rewards
    }
    pub fn add_transaction(&mut self, transaction: &mut Transaction, account_root: Cell) -> Result<()> {
        transaction.set_prev_trans_hash(self.last_trans_hash.clone());
        transaction.set_prev_trans_lt(self.last_trans_lt);
        // log::trace!("{} {}", self.collated_block_descr, debug_transaction(transaction.clone())?);

        self.account_root = account_root;
        self.state_update.new_hash = self.account_root.repr_hash();

        let tr_root = transaction.serialize()?;
        self.last_trans_hash = tr_root.repr_hash();
        self.last_trans_lt = transaction.logical_time();

        self.transactions.setref(
            &transaction.logical_time(),
            &tr_root,
            transaction.total_fees()
        )?;

        if let Some(copyleft_reward) = transaction.copyleft_reward() {
            log::trace!("Copyleft reward {} {} from transaction {}", copyleft_reward.address, copyleft_reward.reward, self.last_trans_hash);
            self.copyleft_rewards.add_copyleft_reward(&copyleft_reward.address, &copyleft_reward.reward)?;
        }

        Ok(())
    }
    pub fn update_public_libraries(&self, libraries: &mut Libraries) -> Result<()> {
        let account = self.read_account()?;
        let new_libs = account.libraries();
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
