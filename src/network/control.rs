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

use crate::{
    collator_test_bundle::CollatorTestBundle, config::{KeyRing, NodeConfigHandler},
    engine_traits::EngineOperations, engine::{Engine, now_duration}, network::node_network::NodeNetwork,
    validator::validator_utils::validatordescr_to_catchain_node,
    validating_utils::{supported_version, supported_capabilities}, shard_states_keeper::PinnedShardStateGuard
};

use adnl::{
    common::{QueryResult, Subscriber, AdnlPeers},
    server::{AdnlServer, AdnlServerConfig}
};
use std::{sync::Arc, str::FromStr};
use ton_api::{
    deserialize_boxed,
    ton::{
        self, PublicKey, TLObject, accountaddress::AccountAddress,
        engine::validator::{
            keyhash::KeyHash, onestat::OneStat, signature::Signature, stats::Stats, Success
        },
        lite_server::configinfo::ConfigInfo,
        raw::{
            shardaccountstate::ShardAccountState,
            shardaccountmeta::ShardAccountMeta,
            appliedshardsinfo::AppliedShardsInfo,
            ShardAccountState as ShardAccountStateBoxed,
            ShardAccountMeta as ShardAccountMetaBoxed,
            AppliedShardsInfo as AppliedShardsInfoBoxed
        },
        rpc::engine::validator::{
            AddAdnlId, AddValidatorAdnlAddress, AddValidatorPermanentKey, AddValidatorTempKey,
            ControlQuery, ExportPublicKey, GenerateKeyPair, Sign, GetBundle, GetFutureBundle
        },
        smc::{
            runtvmresult::RunTvmResultOk,
            runtvmresult::RunTvmResultException,
            RunTvmResult as RunTvmResultBoxed,
        },
    },
    IntoBoxed,
};
use ton_block::{BlockIdExt, MsgAddressInt, Serializable, ShardIdent, MASTERCHAIN_ID, MerkleProof, ShardAccount, Deserializable};
use ton_block_json::serialize_config_param;
use ton_types::{error, fail, KeyId, read_single_root_boc, Result, UInt256, SliceData, HashmapType, AccountId};
use ton_vm::stack::StackItem;
use ton_vm::stack::integer::IntegerData;

enum CallTvmResult {
    Ok(ton_vm::executor::Engine, i32),
    Exception(ton_vm::stack::StackItem, i32),
}

enum RunTvmMode {
    Stack = 0x1,
    C7 = 0x2,
    Messages = 0x4,
    Data = 0x8,
    Code = 0xA,
}

pub struct ControlServer {
    adnl: AdnlServer
}

impl ControlServer {
    pub async fn with_params(
        config: AdnlServerConfig,
        data_source: DataSource,
        key_ring: Arc<dyn KeyRing>,
        node_config: Arc<NodeConfigHandler>,
        network: Option<&NodeNetwork>
    ) -> Result<Self> {
        let ret = Self {
            adnl: AdnlServer::listen(
                config,
                vec![
                    Arc::new(
                        ControlQuerySubscriber::new(data_source, key_ring, node_config, network)?
                    )
                ]
            ).await?
        };
        Ok(ret)
    }
    pub async fn shutdown(self) {
        self.adnl.shutdown().await
    }
}

pub trait StatusReporter: Send + Sync {
    fn get_report(&self) -> u32;
}

pub enum DataSource {
    Engine(Arc<dyn EngineOperations>),
    Status(Arc<dyn StatusReporter>)
}

struct ControlQuerySubscriber {
    data_source: DataSource,
    key_ring: Arc<dyn KeyRing>,
    config: Arc<NodeConfigHandler>,
    public_overlay_adnl_id: Option<Arc<KeyId>>
}

impl ControlQuerySubscriber {

    fn new(
        data_source: DataSource,
        key_ring: Arc<dyn KeyRing>,
        config: Arc<NodeConfigHandler>,
        network: Option<&NodeNetwork>,
    ) -> Result<Self> {
        let key_id = if let Some (network) = network {
            Some(network.get_key_id_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?)
        } else {
            None
        };
        let ret = Self {
            data_source,
            key_ring,
            config,
            public_overlay_adnl_id: key_id
        };
        Ok(ret)
    }

    fn engine(&self) -> Result<&Arc<dyn EngineOperations>> {
        match self.data_source {
            DataSource::Engine(ref engine) => Ok(engine),
            _ => fail!("`engine is not set`")
        }
    }

    async fn get_all_config_params(&self) -> Result<ConfigInfo> {
        let engine = self.engine()?;
        let mc_state = engine.load_last_applied_mc_state().await?;
        let block_id = mc_state.block_id();
        let config_params = mc_state.config_params()?;
        let config_info = ConfigInfo {
            mode: 0,
            id: block_id.clone(),
            state_proof: ton::bytes(vec!()),
            config_proof: ton::bytes(config_params.write_to_bytes()?)
        };
        Ok(config_info)
    }

    async fn get_config_params(&self, param_number: u32) -> Result<ConfigInfo> {
        let engine = self.engine()?;
        let mc_state = engine.load_last_applied_mc_state().await?;
        let config_params = mc_state.config_params()?;
        let config_param = serialize_config_param(config_params, param_number)?;
        let config_info = ConfigInfo {
            mode: 0,
            id: mc_state.block_id().clone(),
            state_proof: ton::bytes(vec!()),
            config_proof: ton::bytes(config_param.into_bytes())
        };
        Ok(config_info)
    }

    fn stack_item_from_tl(item: ton::tvm::StackEntry) -> Result<ton_vm::stack::StackItem> {
        Ok(match item {
            ton::tvm::StackEntry::Tvm_StackEntryBuilder(ton::tvm::stackentry::StackEntryBuilder { builder }) => {
                ton_vm::stack::StackItem::builder(ton_types::BuilderData::from_cell(&read_single_root_boc(&builder.bytes.0)?)?)
            },
            ton::tvm::StackEntry::Tvm_StackEntryCell(ton::tvm::stackentry::StackEntryCell { cell }) => {
                ton_vm::stack::StackItem::cell(read_single_root_boc(&cell.bytes.0)?)
            },
            ton::tvm::StackEntry::Tvm_StackEntrySlice(ton::tvm::stackentry::StackEntrySlice { slice }) => {
                let cell = read_single_root_boc(&slice.bytes)?;
                let slice = ton_vm::stack::slice_deserialize(&mut SliceData::load_cell(cell)?)?;
                ton_vm::stack::StackItem::slice(slice)
            },
            ton::tvm::StackEntry::Tvm_StackEntryList(ton::tvm::stackentry::StackEntryList { list }) => {
                let mut list = list.only().elements.0;
                let mut result = ton_vm::stack::StackItem::None;
                while let Some(item) = list.pop() {
                    result = ton_vm::stack::StackItem::tuple(vec![Self::stack_item_from_tl(item)?, result]);
                }   
                result
            },
            ton::tvm::StackEntry::Tvm_StackEntryTuple(ton::tvm::stackentry::StackEntryTuple { tuple }) => {
                let mut result = vec![];
                for item in tuple.only().elements.0 {
                    result.push(Self::stack_item_from_tl(item)?);
                }
                ton_vm::stack::StackItem::tuple(result)
            },
            ton::tvm::StackEntry::Tvm_StackEntryNumber(ton::tvm::stackentry::StackEntryNumber { number }) => {
                if number.number().eq_ignore_ascii_case("nan") {
                    ton_vm::stack::StackItem::nan()
                } else {
                    ton_vm::stack::StackItem::integer(
                        ton_vm::stack::integer::IntegerData::from_str(number.number())
                            .map_err(|_| error!("invalid number stack item {}", number.number()))?
                    )
                }
            },
            ton::tvm::StackEntry::Tvm_StackEntryNull | ton::tvm::StackEntry::Tvm_StackEntryUnsupported => {
                ton_vm::stack::StackItem::None
            },
        })
    }

    fn convert_stack_from_tl(stack: Vec<ton::tvm::StackEntry>) -> Result<ton_vm::stack::Stack> {
        let mut result = ton_vm::stack::Stack::new();
        for entry in stack {
            result.push(Self::stack_item_from_tl(entry)?);
        }
        Ok(result)
    }

    fn try_tuple_to_list(
        mut tuple_items: &[ton_vm::stack::StackItem],
    ) -> Result<Option<Vec<ton::tvm::StackEntry>>> {
        let mut result = vec![];
        let item_type = tuple_items.get(0).map(|item| std::mem::discriminant(item));

        loop {
            if tuple_items.len() != 2 || Some(std::mem::discriminant(&tuple_items[0])) != item_type {
                return Ok(None);
            }
            result.push(Self::stack_item_to_tl(&tuple_items[0])?);
            match &tuple_items[1] {
                ton_vm::stack::StackItem::Tuple(items) => tuple_items = items.as_ref(),
                ton_vm::stack::StackItem::None => break,
                _ => return Ok(None),
            }
        }

        Ok(Some(result))
    }

    fn stack_item_to_tl(item: &ton_vm::stack::StackItem) -> Result<ton::tvm::StackEntry> {
        Ok(match item {
            ton_vm::stack::StackItem::Builder(builder) => {
                ton::tvm::StackEntry::Tvm_StackEntryBuilder(ton::tvm::stackentry::StackEntryBuilder {
                    builder: ton::tvm::builder::Builder {
                        bytes: ton_types::boc::write_boc(&builder.as_ref().clone().into_cell()?)?.into()
                    }
                })
            }
            ton_vm::stack::StackItem::Continuation(_) => {
                ton::tvm::StackEntry::Tvm_StackEntryUnsupported
            }
            ton_vm::stack::StackItem::Integer(int) => {
                ton::tvm::StackEntry::Tvm_StackEntryNumber(ton::tvm::stackentry::StackEntryNumber {
                    number: ton_api::ton::tvm::Number::Tvm_NumberDecimal(ton::tvm::numberdecimal::NumberDecimal {
                        number: int.to_str_radix(10)
                    })
                })
            }
            ton_vm::stack::StackItem::None => {
                ton::tvm::StackEntry::Tvm_StackEntryNull
            }
            ton_vm::stack::StackItem::Cell(cell) => {
                ton::tvm::StackEntry::Tvm_StackEntryCell(ton::tvm::stackentry::StackEntryCell {
                    cell: ton::tvm::cell::Cell {
                        bytes: ton_api::ton::bytes(ton_types::boc::write_boc(&cell)?)
                    }
                })
            }
            ton_vm::stack::StackItem::Slice(slice) => {
                ton::tvm::StackEntry::Tvm_StackEntrySlice(ton::tvm::stackentry::StackEntrySlice {
                    slice: ton::tvm::slice::Slice {
                        bytes: ton_types::boc::write_boc(&ton_vm::stack::slice_serialize(&slice)?.into_cell()?)?.into()
                    }
                })
            }
            ton_vm::stack::StackItem::Tuple(items) => {
                if let Some(elements) = Self::try_tuple_to_list(items)? {
                    ton::tvm::StackEntry::Tvm_StackEntryList(ton::tvm::stackentry::StackEntryList {
                        list: ton_api::ton::tvm::List::Tvm_List(ton::tvm::list::List { elements: elements.into() })
                    })
                } else {
                    let mut elements = Vec::with_capacity(items.len());
                    for item in items.as_ref() {
                        elements.push(Self::stack_item_to_tl(item)?);
                    }
                    ton::tvm::StackEntry::Tvm_StackEntryTuple(ton::tvm::stackentry::StackEntryTuple {
                        tuple: ton_api::ton::tvm::Tuple::Tvm_Tuple(ton::tvm::tuple::Tuple { elements: elements.into() })
                    })
                }

            }
        })
    }

    fn convert_stack_to_tl(stack: &ton_vm::stack::Stack) -> Result<Vec<ton::tvm::StackEntry>> {
        let mut result = Vec::with_capacity(stack.depth());
        for item in stack.iter() {
            result.push(Self::stack_item_to_tl(item)?);
        }
        Ok(result)
    }

    async fn run_account_on_tvm(
        &self,
        stack: ton_vm::stack::Stack,
        account: ShardAccount,
        state_guard: PinnedShardStateGuard,
        mode: i32,
    ) -> Result<RunTvmResultBoxed> {
        let config_state_guard = if state_guard.state().block_id().shard().is_masterchain() {
            state_guard.clone()
        } else {
            let master_ref = state_guard
                .state()
                .state()?
                .master_ref()
                .ok_or_else(|| error!("No master ref in shard state"))?;
            self.engine()?.load_and_pin_state(&master_ref.master.clone().master_block_id().1).await?
        };

        let config = config_state_guard.state().config_params()?;
        let mut account = account.read_account()?;

        match Self::call_tvm(&mut account, stack, config)? {
            CallTvmResult::Ok(engine, exit_code) => {
                let mut result = RunTvmResultOk::default();
                result.block_root_hash = state_guard.state().block_id().root_hash.clone();
                result.mode = mode;
                result.exit_code = exit_code;

                if mode & RunTvmMode::Stack as i32 != 0 {
                    result.stack = Some(Self::convert_stack_to_tl(engine.stack())?.into());
                }
                if mode & RunTvmMode::C7 as i32 != 0 {
                    result.init_c7 = Some(Self::stack_item_to_tl(engine.ctrl(7)?)?);
                }
                if mode & RunTvmMode::Code as i32 != 0 {
                    result.code = Some(ton_types::boc::write_boc(&account.get_code().unwrap_or_default())?.into());
                }
                if mode & RunTvmMode::Data as i32 != 0 {
                    result.data = Some(ton_types::boc::write_boc(&account.get_data().unwrap_or_default())?.into());
                }
                if mode & RunTvmMode::Messages as i32 != 0 {
                    let actions_cell = engine.get_actions().as_cell()?.clone();
                    let actions = ton_block::OutActions::construct_from_cell(actions_cell)?;
                
                    let mut msgs = vec![];
                    for action in actions.iter().rev() {
                        match action {
                            ton_block::OutAction::SendMsg { out_msg, .. } => {
                                msgs.push(out_msg.write_to_bytes()?.into());
                            }
                            _ => {}
                        }
                    }

                    result.messages = Some(msgs.into());
                }

                Ok(RunTvmResultBoxed::Smc_RunTvmResultOk(result))
            }
            CallTvmResult::Exception(exit_arg, exit_code) => {
                Ok(RunTvmResultBoxed::Smc_RunTvmResultException(
                    RunTvmResultException {
                        block_root_hash: state_guard.state().block_id().root_hash.clone() ,
                        exit_code,
                        exit_arg: Self::stack_item_to_tl(&exit_arg)?,
                    }
                ))
            }
        }
    }

    async fn run_tvm_stack_by_block(&self, params: ton::rpc::smc::RunTvmByBlock) -> Result<RunTvmResultBoxed> {
        let stack = Self::convert_stack_from_tl(params.stack.0)?;

        let (account, state_guard) = self.find_account_by_block(&params.account_id.clone().into(), &params.block_root_hash)
            .await?
            .ok_or_else(|| error!("Account {} does not exist in block {}", params.account_id, params.block_root_hash))?;

        self.run_account_on_tvm(stack, account, state_guard, params.mode).await
    }

    async fn run_tvm_stack(&self, params: ton::rpc::smc::RunTvm) -> Result<RunTvmResultBoxed> {
        let stack = Self::convert_stack_from_tl(params.stack.0)?;

        let address: MsgAddressInt = params.account_address.account_address.parse()?;
        let (account, state_guard) = self.find_account(&address)
            .await?
            .ok_or_else(|| error!("Account {} does not exist", params.account_address.account_address))?;

        self.run_account_on_tvm(stack, account, state_guard, params.mode).await
    }

    fn create_stack_with_message(
        account: &ShardAccount,
        message: &ton_block::Message,
        message_cell: ton_types::Cell,
    ) -> Result<ton_vm::stack::Stack> {
        let acc_balance = account.read_account()?.balance().cloned().unwrap_or_default();

        let mut stack = ton_vm::stack::Stack::new();
        stack
            .push(ton_vm::int!(acc_balance.grams.as_u128()))
            .push(ton_vm::int!(0))
            .push(ton_vm::stack::StackItem::Cell(message_cell))
            .push(ton_vm::stack::StackItem::Slice(message.body().unwrap_or_default()))
            .push(ton_vm::int!(-1));

        Ok(stack)
    }

    async fn run_tvm_msg_by_block(&self, params: ton::rpc::smc::RunTvmMsgByBlock) -> Result<RunTvmResultBoxed> {
        let msg_cell = ton_types::read_single_root_boc(&params.message.0)?;
        let msg = ton_block::Message::construct_from_cell(msg_cell.clone())?;
        let header = msg.ext_in_header()
            .ok_or_else(|| error!("Wrong message type: only external inbound messages supported"))?;

        let (account, state_guard) = self.find_account_by_block(&header.dst.address(), &params.block_root_hash)
            .await?
            .ok_or_else(|| error!("Account {} does not exist in block {}", header.dst, params.block_root_hash))?;
        
        let stack = Self::create_stack_with_message(&account, &msg, msg_cell)?;
        self.run_account_on_tvm(stack, account, state_guard, params.mode).await
    }

    async fn run_tvm_msg(&self, params: ton::rpc::smc::RunTvmMsg) -> Result<RunTvmResultBoxed> {
        let msg_cell = ton_types::read_single_root_boc(&params.message.0)?;
        let msg = ton_block::Message::construct_from_cell(msg_cell.clone())?;
        let header = msg.ext_in_header()
            .ok_or_else(|| error!("Wrong message type: only external inbound messages supported"))?;

        let (account, state_guard) = self.find_account(&header.dst)
            .await?
            .ok_or_else(|| error!("Account {} does not exist", header.dst))?;

        let stack = Self::create_stack_with_message(&account, &msg, msg_cell)?;
        self.run_account_on_tvm(stack, account, state_guard, params.mode).await
    }
    
    fn call_tvm(
        account: &mut ton_block::Account,
        stack: ton_vm::stack::Stack,
        config: &ton_block::ConfigParams,
    ) -> Result<CallTvmResult> {
        let code = account
            .get_code()
            .ok_or_else(|| error!("Account has no code"))?;
        let data = account.get_data().unwrap_or_default();
        let addr = account
            .get_addr()
            .ok_or_else(|| error!("Account has no address"))?;
        let balance = account
            .balance()
            .ok_or_else(|| error!("Account has no balance"))?;

        let mut ctrls = ton_vm::stack::savelist::SaveList::new();
        ctrls
            .put(4, &mut ton_vm::stack::StackItem::Cell(data))
            .map_err(|err| error!("can not put data to registers: {}", err))?;

        let smc_info = ton_vm::SmartContractInfo {
            capabilities: config.capabilities(),
            myself: SliceData::load_builder(addr.write_to_new_cell().unwrap_or_default()).unwrap(),
            balance: balance.clone(),
            config_params: config.config_params.data().cloned(),
            unix_time: now_duration().as_secs() as u32,
            ..Default::default()
        };
        ctrls
            .put(7, &mut smc_info.into_temp_data_item())?;

        let mut engine = ton_vm::executor::Engine::with_capabilities(
            config.capabilities()
        ).setup(
            SliceData::load_cell(code)?,
            Some(ctrls),
            Some(stack),
            None,
        );

        match engine.execute() {
            Err(err) => {
                let exception = ton_vm::error::tvm_exception(err)?;
                let code = if let Some(code) = exception.custom_code() {
                    code
                } else {
                    !(exception
                        .exception_code()
                        .unwrap_or(ton_types::ExceptionCode::UnknownError) as i32)
                };

                Ok(CallTvmResult::Exception(exception.value, code))
            }
            Ok(exit_code) => match engine.get_committed_state().get_root() {
                ton_vm::stack::StackItem::Cell(data) => {
                    account.set_data(data.clone());
                    Ok(CallTvmResult::Ok(engine, exit_code))
                }
                _ => Err(error!("invalid committed state")),
            },
        }
    }

    async fn get_account_state(&self, address: AccountAddress) -> Result<ShardAccountStateBoxed> {
        let address: MsgAddressInt = address.account_address.parse()?;
        Self::convert_account_state(self.find_account(&address).await?)
    }

    async fn get_account_by_block(&self, account_id: UInt256, block_root_hash: UInt256) -> Result<ShardAccountStateBoxed> {
        Self::convert_account_state(self.find_account_by_block(&account_id.into(), &block_root_hash).await?)
    }

    async fn get_account_meta(&self, address: AccountAddress) -> Result<ShardAccountMetaBoxed> {
        let address: MsgAddressInt = address.account_address.parse()?;
        Self::convert_account_meta(self.find_account(&address).await?)
    }

    async fn get_account_meta_by_block(&self, account_id: UInt256, block_root_hash: UInt256) -> Result<ShardAccountMetaBoxed> {
        Self::convert_account_meta(self.find_account_by_block(&account_id.into(), &block_root_hash).await?)
    }

    async fn find_account(
        &self, addr: &MsgAddressInt
    ) -> Result<Option<(ShardAccount, PinnedShardStateGuard)>> {
        let engine = self.engine()?;
        let state = if addr.is_masterchain() {
            let mc_block_id = engine.load_last_applied_mc_block_id()?
                .ok_or_else(|| error!("Cannot load last_applied_mc_block_id!"))?;
            engine.load_and_pin_state(&mc_block_id).await?
        } else {
            let mc_block_id = engine.load_shard_client_mc_block_id()?;
            let mc_block_id = mc_block_id.ok_or_else(
                || error!("Cannot load shard_client_mc_block_id!")
            )?;
            let mc_state = engine.load_and_pin_state(&mc_block_id).await?;
            let mut shard_state = None;
            for id in mc_state.state().top_blocks(addr.workchain_id())? {
                if id.shard().contains_account(addr.address().clone())? {
                    shard_state = engine.load_and_pin_state(&id).await.ok();
                    break;
                }
            }
            shard_state.ok_or_else(
                || error!("Cannot find actual shard for account {}", addr)
            )?
        };
        Ok(state.state().shard_account(&addr.address())?.map(|acc| (acc, state)))
    }

    async fn find_account_by_block(
        &self, account_id: &AccountId, block_root_hash: &UInt256
    ) -> Result<Option<(ShardAccount, PinnedShardStateGuard)>> {
        let engine = self.engine()?;
        let block_id = engine.find_full_block_id(block_root_hash)?
            .ok_or_else(|| error!("Cannot find full block id by root hash"))?;
        let shard_state = engine.load_and_pin_state(&block_id).await?;
        Ok(shard_state.state().shard_account(account_id)?.map(|acc| (acc, shard_state)))
    }

    fn convert_account_state(
        shard_account: Option<(ShardAccount, PinnedShardStateGuard)>
    ) -> Result<ShardAccountStateBoxed> {
        Ok(match shard_account {
            Some((account, _state_guard)) => ShardAccountStateBoxed::Raw_ShardAccountState(
                ShardAccountState {
                    shard_account: ton_api::ton::bytes(account.write_to_bytes()?)
                }
            ),
            None => ShardAccountStateBoxed::Raw_ShardAccountNone
        })
    }

    fn convert_account_meta(
        shard_account: Option<(ShardAccount, PinnedShardStateGuard)>
    ) -> Result<ShardAccountMetaBoxed> {
        Ok(match shard_account {
            Some((shard_account, _state_guard)) => {
                let account = shard_account.read_account()?;
                let code = account.get_code().map(|cell| cell.repr_hash());
                let data = account.get_data().map(|cell| cell.repr_hash());
                let libs = account.libraries().root().map(|cell| cell.repr_hash());

                let cell = shard_account.account_cell();
                let proof = MerkleProof::create(
                    &cell,
                    |hash| Some(hash) != code.as_ref() && Some(hash) != data.as_ref() && Some(hash) != libs.as_ref()
                ).unwrap();
                ShardAccountMetaBoxed::Raw_ShardAccountMeta(ShardAccountMeta {
                    shard_account_meta: ton_api::ton::bytes(proof.write_to_bytes()?)
                })
            },
            None => ShardAccountMetaBoxed::Raw_ShardAccountMetaNone
        })
    }

    async fn get_applied_shards_info(&self) -> Result<AppliedShardsInfoBoxed> {
        let engine = self.engine()?;
        let mc_block_id = engine.load_last_applied_mc_block_id()?
            .ok_or_else(|| error!("Cannot load load_last_applied_mc_block_id"))?;
        let shards_mc_block_id = engine.load_shard_client_mc_block_id()?
            .ok_or_else(|| error!("Cannot load shard_client_mc_block_id"))?;
        let mut applied_blocks = engine.load_state(&shards_mc_block_id).await?.top_blocks_all()?;
        applied_blocks.push(mc_block_id.as_ref().clone());
        Ok(AppliedShardsInfoBoxed::Raw_AppliedShardsInfo(AppliedShardsInfo { shards: applied_blocks.into() }))
    }

    fn convert_sync_status(&self, sync_status: u32 ) -> String {
        match sync_status {
            Engine::SYNC_STATUS_START_BOOT => "start_boot".to_string(),
            Engine::SYNC_STATUS_LOAD_MASTER_STATE => "load_master_state".to_string(),
            Engine::SYNC_STATUS_LOAD_SHARD_STATES => "load_shard_states".to_string(),
            Engine::SYNC_STATUS_FINISH_BOOT => "finish_boot".to_string(),
            Engine::SYNC_STATUS_SYNC_BLOCKS => "synchronization_by_blocks".to_string(),
            Engine::SYNC_STATUS_FINISH_SYNC => "synchronization_finished".to_string(),
            Engine::SYNC_STATUS_CHECKING_DB => "checking_db".to_string(),
            Engine::SYNC_STATUS_DB_BROKEN => "db_broken".to_string(),
            _ => "no_set_status".to_string()
        }
    }

    fn block_id_to_json(block_id: &BlockIdExt) -> String {
        serde_json::json!({
            "shard":  block_id.shard().to_string(),
            "seq_no": block_id.seq_no(),
            "rh":     format!("{:x}", block_id.root_hash),
            "fh":     format!("{:x}", block_id.file_hash)
        }).to_string()
    }

    fn add_stats(stats: &mut Vec<OneStat>, key: impl ToString, value: impl ToString) {
        stats.push(OneStat {
            key: key.to_string(),
            value: value.to_string()
        })
    }

    fn statistics_to_json(
        map: &lockfree::map::Map<ShardIdent, u64>,
        now: i64,
        new_format: bool
    ) -> String {
        let mut json_map = serde_json::Map::new();
        for item in map.iter() {
            let value = if new_format {
                match *item.val() {
                    0 => -1,
                    value => now - value as i64
                }.into()
            } else {
                match *item.val() {
                    0 => "never".to_string(),
                    value => format!("{} sec ago", now - value as i64)
                }.into()
            };
            json_map.insert(item.key().to_string(), value);
        }
        format!("{:#}", serde_json::Value::from(json_map))
    }

    fn get_shards_time_diff(engine: &Arc<dyn EngineOperations>, now: u32) -> Result<u32> {
        let shard_client_mc_block_id = engine.load_shard_client_mc_block_id()?
            .ok_or_else(|| error!("Cannot load shard_mc_block_id"))?;
        let shard_client_mc_block_handle = engine.load_block_handle(&shard_client_mc_block_id)?
            .ok_or_else(|| error!("Cannot load handle for block {}", &shard_client_mc_block_id))?;
        Ok(now - shard_client_mc_block_handle.gen_utime()?)
    }

    async fn get_selected_stats(&self, filter: Option<&str>) -> Result<Stats> {

        let mut stats = Vec::new();
        let new_format = filter.is_some();

        // sync status
        let sync_status = match &self.data_source {
            DataSource::Engine(engine) => engine.get_sync_status(),
            DataSource::Status(status) => status.get_report()
        };
        let sync_status = format!("\"{}\"", self.convert_sync_status(sync_status));
        Self::add_stats(
            &mut stats,
            if new_format {
                "node_status"
            } else {
                "sync_status"
            },
            sync_status
        );
        if let DataSource::Status(_) = &self.data_source {
            return Ok(Stats {stats: stats.into()})
        }

        let engine = self.engine()?;
        let now = engine.now();

        let mc_block_id = if let Some(id) = engine.load_last_applied_mc_block_id()? {
            id
        } else {
            Self::add_stats(&mut stats, "masterchainblock", "\"not set\"");
            return Ok(Stats {stats: stats.into()})
        };

        let mc_block_handle = engine.load_block_handle(&mc_block_id)?
            .ok_or_else(|| error!("Cannot load handle for block {}", &mc_block_id))?;

        // masterchainblocktime
        Self::add_stats(&mut stats, "masterchainblocktime", mc_block_handle.gen_utime()?);

        // masterchainblocknumber
        Self::add_stats(&mut stats, "masterchainblocknumber", mc_block_handle.id().seq_no());

        Self::add_stats(&mut stats, "node_version", format!("\"{}\"", env!("CARGO_PKG_VERSION")));
        let public_overlay_adnl_id = self.public_overlay_adnl_id.as_ref().ok_or_else(||
            error!("Public overlay key id didn`t set!")
        )?;
        Self::add_stats(&mut stats, "public_overlay_key_id", format!("\"{}\"", &public_overlay_adnl_id));

        if new_format {
            Self::add_stats(&mut stats, "supported_block", supported_version());
            Self::add_stats(&mut stats, "supported_capabilities", supported_capabilities());
        }

        // timediff
        let diff = now - mc_block_handle.gen_utime()?;
        Self::add_stats(&mut stats, "timediff", diff);

        // shards timediff
        match Self::get_shards_time_diff(engine, now) {
            Err(_) => Self::add_stats(&mut stats, "shards_timediff", "\"unknown\""),
            Ok(shards_timediff) => Self::add_stats(&mut stats, "shards_timediff", shards_timediff),
        };

        let mc_state = engine.load_last_applied_mc_state().await.ok();

        // global network ID
        if new_format {
            if let Some(mc_state) = &mc_state {
                Self::add_stats(&mut stats, "global_id", mc_state.state()?.global_id())
            } else {
                Self::add_stats(&mut stats, "global_id", "\"unknown\"")
            }
        }

        // in_current_vset_p34
        let adnl_ids = self.config.get_actual_validator_adnl_ids()?;
        if let Some(mc_state) = &mc_state {
            let current = mc_state.config_params()?.validator_set()?.list().iter().any(|val| {
                let catchain_node = validatordescr_to_catchain_node(val);
                let is_validator = adnl_ids.contains(&catchain_node.adnl_id);
                if is_validator {
                    Self::add_stats(&mut stats,
                        "current_vset_p34_adnl_id",
                        format!("\"{}\"", &catchain_node.adnl_id)
                    );
                }
                is_validator
            });
            Self::add_stats(&mut stats, "in_current_vset_p34", current)
        } else {
            Self::add_stats(&mut stats, "in_current_vset_p34", "\"unknown\"")
        }

        // in_next_vset_p36
        if let Some(mc_state) = &mc_state {
            let next = mc_state.config_params()?.next_validator_set()?.list().iter().any(|val| {
                let catchain_node = validatordescr_to_catchain_node(val);
                let is_validator = adnl_ids.contains(&catchain_node.adnl_id);
                if is_validator {
                    Self::add_stats(&mut stats,
                        "next_vset_p36_adnl_id",
                        format!("\"{}\"", &catchain_node.adnl_id)
                    );
                }
                is_validator
            });
            Self::add_stats(&mut stats, "in_next_vset_p36", next)
        } else {
            Self::add_stats(&mut stats, "in_next_vset_p36", "\"unknown\"")
        }

        let value = match engine.load_last_applied_mc_block_id() {
            Ok(Some(block_id)) => Self::block_id_to_json(&block_id),
            Ok(None) => "\"no last applied masterchain block{}\"".to_string(),
            Err(err) => format!("\"{}\"", err)
        };
        Self::add_stats(&mut stats, "last_applied_masterchain_block_id", value);

        let value = match engine.processed_workchain() {
            Some(MASTERCHAIN_ID) => "\"masterchain\"".to_string(),
            Some(workchain_id) => format!("\"{}\"", workchain_id),
            None => "\"not specified\"".to_string(),
        };
        Self::add_stats(&mut stats, "processed_workchain", value);

        let value = Self::statistics_to_json(
            engine.last_validation_time(),
            now as i64,
            new_format
        );
        if new_format {
            Self::add_stats(&mut stats, "last_validation_ago_sec", value)
        } else {
            Self::add_stats(&mut stats, "validation_stats", value)
        }

        let value = Self::statistics_to_json(
            engine.last_collation_time(),
            now as i64,
            new_format
        );
        if new_format {
            Self::add_stats(&mut stats, "last_collation_ago_sec", value)
        } else {
            Self::add_stats(&mut stats, "collation_stats", value)
        }

        // tps_10
        if let Ok(tps) = engine.calc_tps(10) {
            Self::add_stats(&mut stats, "tps_10", tps);
        }

        // tps_300
        if let Ok(tps) = engine.calc_tps(300) {
            Self::add_stats(&mut stats, "tps_300", tps);
        }

        Self::add_stats(&mut stats, "validation_status", format!("\"{:?}\"", engine.validation_status()));

        Ok(Stats { stats: stats.into() })

    }

    async fn process_generate_keypair(&self) -> Result<KeyHash> {
        let ret = KeyHash {
            key_hash: UInt256::with_array(self.key_ring.generate().await?)
        };
        Ok(ret)
    }

    fn export_public_key(&self, key_hash: &[u8; 32]) -> Result<PublicKey> {
        let private = self.key_ring.find(key_hash)?;
        (&private).try_into()
    }

    fn process_sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Signature> {
        let sign = self.key_ring.sign_data(key_hash, data)?;
        Ok(Signature {signature: ton::bytes(sign)})
    }

    async fn add_validator_permanent_key(
        &self,
        key_hash: &[u8; 32],
        election_date: ton::int,
        _ttl: ton::int
    ) -> Result<Success> {
        self.config.add_validator_key(key_hash, election_date).await?;
        Ok(Success::Engine_Validator_Success)
    }

    fn add_validator_temp_key(
        &self,
        _perm_key_hash: &[u8; 32],
        _key_hash: &[u8; 32],
        _ttl: ton::int
    ) -> Result<Success> {
        Ok(Success::Engine_Validator_Success)
    }

    async fn add_validator_adnl_address(
        &self,
        perm_key_hash: &[u8; 32],
        key_hash: &[u8; 32],
        _ttl: ton::int
    ) -> Result<Success> {
        self.config.add_validator_adnl_key(perm_key_hash, key_hash).await?;
        Ok(Success::Engine_Validator_Success)
    }

    fn add_adnl_address(&self, _key_hash: &[u8; 32], _category: ton::int) -> Result<Success> {
        Ok(Success::Engine_Validator_Success)
    }

    async fn prepare_bundle(&self, block_id: BlockIdExt) -> Result<Success> {
        if let DataSource::Engine(ref engine) = self.data_source {
            let bundle = CollatorTestBundle::build_with_ethalon(&block_id, engine).await?;
            tokio::task::spawn_blocking(move || {
                bundle.save("target/bundles").ok();
            });
        }
        Ok(Success::Engine_Validator_Success)
    }

    async fn prepare_future_bundle(&self, prev_block_ids: Vec<BlockIdExt>) -> Result<Success> {
        if let DataSource::Engine(ref engine) = self.data_source {
            let bundle = CollatorTestBundle::build_for_collating_block(
                prev_block_ids, engine
            ).await?;
            tokio::task::spawn_blocking(move || {
                bundle.save("target/bundles").ok();
            });
        }
        Ok(Success::Engine_Validator_Success)
    }

    async fn redirect_external_message(&self, message_data: &[u8]) -> Result<Success> {
        let engine = self.engine()?;
        let id = read_single_root_boc(message_data)?.repr_hash();
        engine.redirect_external_message(message_data, id).await?;
        Ok(Success::Engine_Validator_Success)
    }

    fn set_states_gc_interval(&self, interval_ms: u32) -> Result<Success> {
        self.engine()?.adjust_states_gc_interval(interval_ms);
        self.config.store_states_gc_interval(interval_ms);
        Ok(Success::Engine_Validator_Success)
    }

    async fn try_consume_query_impl(&self, object: TLObject, _peers: &AdnlPeers) -> Result<QueryResult> {
        log::debug!("recieve object (control server): {:?}", object);
        let query = match object.downcast::<ControlQuery>() {
            Ok(query) => deserialize_boxed(&query.data[..])?,
            Err(object) => return Ok(QueryResult::Rejected(object))
        };
        log::debug!("query (control server): {:?}", query);
        let query = match query.downcast::<ton::rpc::raw::GetShardAccountState>() {
            Ok(account) => {
                let answer = self.get_account_state(account.account_address).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::raw::GetShardAccountMeta>() {
            Ok(account) => {
                let answer = self.get_account_meta(account.account_address).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::raw::GetAccountByBlock>() {
            Ok(account) => {
                let answer = self.get_account_by_block(account.account_id, account.block_root_hash).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::raw::GetAccountMetaByBlock>() {
            Ok(account) => {
                let answer = self.get_account_meta_by_block(account.account_id, account.block_root_hash).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::raw::GetAppliedShardsInfo>() {
            Ok(_) => {
                let answer = self.get_applied_shards_info().await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::smc::RunTvm>() {
            Ok(params) => {
                let answer = self.run_tvm_stack(params).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::smc::RunTvmByBlock>() {
            Ok(params) => {
                let answer = self.run_tvm_stack_by_block(params).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::smc::RunTvmMsg>() {
            Ok(params) => {
                let answer = self.run_tvm_msg(params).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::smc::RunTvmMsgByBlock>() {
            Ok(params) => {
                let answer = self.run_tvm_msg_by_block(params).await?;
                return QueryResult::consume_boxed(
                    answer,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<GenerateKeyPair>() {
            Ok(_) => return QueryResult::consume(
                self.process_generate_keypair().await?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<ExportPublicKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.export_public_key(query.key_hash.as_slice())?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<Sign>() {
            Ok(query) => return QueryResult::consume(
                self.process_sign_data(query.key_hash.as_slice(), &query.data)?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorPermanentKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_permanent_key(
                    query.key_hash.as_slice(), query.election_date, query.ttl
                ).await?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorTempKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_temp_key(
                    query.permanent_key_hash.as_slice(), query.key_hash.as_slice(), query.ttl
                )?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorAdnlAddress>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_adnl_address(
                    query.permanent_key_hash.as_slice(), query.key_hash.as_slice(), query.ttl
                ).await?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddAdnlId>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_adnl_address(query.key_hash.as_slice(), query.category)?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<GetBundle>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.prepare_bundle(query.block_id.clone()).await?,
                #[cfg(feature = "telemetry")]
                None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<GetFutureBundle>() {
            Ok(query) => {
                let prev_block_ids = query.prev_block_ids.iter().map(
                    |id| id.clone()
                ).collect();
                return QueryResult::consume_boxed(
                    self.prepare_future_bundle(prev_block_ids).await?,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::SendMessage>() {
            Ok(query) => {
                let message_data = query.body.0;
                return QueryResult::consume_boxed(
                    self.redirect_external_message(&message_data).await?,
                    #[cfg(feature = "telemetry")]
                    None
                )
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::GetConfigParams>() {
            Ok(query) => {
                let param_number = query.param_list.iter().next().ok_or_else(|| error!("Invalid param_number"))?;
                let answer = self.get_config_params(*param_number as u32).await?;

                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::GetConfigAll>() {
            Ok(_) => {
                let answer = self.get_all_config_params().await?;
                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetStats>() {
            Ok(_) => {
                let answer = self.get_selected_stats(None).await?;
                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetSelectedStats>() {
            Ok(get_stats) => {
                let answer = self.get_selected_stats(Some(&get_stats.filter)).await?;
                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::SetStatesGcInterval>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.set_states_gc_interval(query.interval_ms as u32)?,
                    #[cfg(feature = "telemetry")]
                    None
                )
            }
            Err(query) => query
        };
        log::warn!("Unsupported ControlQuery (control server): {:?}", query);
        Ok(QueryResult::Rejected(query))
    }
}

#[async_trait::async_trait]
impl Subscriber for ControlQuerySubscriber {
    async fn try_consume_query(&self, object: TLObject, peers: &AdnlPeers) -> Result<QueryResult> {
        let now = std::time::Instant::now();
        let result = match self.try_consume_query_impl(object, peers).await {
            Ok(result) => Ok(result),
            Err(err) => QueryResult::consume_boxed(
                ton::engine::validator::ControlQueryError::Engine_Validator_ControlQueryError(
                    ton::engine::validator::controlqueryerror::ControlQueryError {
                        code: -1,
                        message: err.to_string()
                    }
                ),
                #[cfg(feature = "telemetry")]
                None
            )
        };
        log::trace!("Control server operation {} TIME", now.elapsed().as_millis());
        result
    }
}

