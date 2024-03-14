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

use adnl::{
    common::TaggedTlObject, client::{AdnlClient, AdnlClientConfig, AdnlClientConfigJson}
};
use std::{convert::TryInto, env, str::FromStr, time::Duration};
use ton_api::{
    serialize_boxed,
    ton::{
        self, TLObject, 
        accountaddress::AccountAddress, engine::validator::{ControlQueryError, onestat::OneStat}, 
        raw::ShardAccountState, rpc::engine::validator::ControlQuery
    }
};
#[cfg(feature = "telemetry")]
use ton_api::tag_from_bare_object;
use ton_block::{
    AccountStatus, Deserializable, BlockIdExt, Serializable, ShardAccount
};
use ton_types::{
    error, fail, base64_decode, base64_encode, BuilderData, Ed25519KeyOption, Result, 
    SliceData, UInt256, write_boc
};

include!("../common/src/test.rs");

trait SendReceive {
    fn send<Q: ToString>(params: impl Iterator<Item = Q>) -> Result<TLObject>;
    fn receive<Q: ToString>(
        answer: TLObject, 
        _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        downcast::<ton_api::ton::engine::validator::Success>(answer)?;
        Ok(("success".to_string(), vec![]))
    }
}

trait ConsoleCommand: SendReceive {
    fn name() -> &'static str;
    fn help() -> &'static str;
}

macro_rules! commands {
    ($($command: ident, $name: literal, $help: literal)*) => {
        $(
            struct $command;
            impl ConsoleCommand for $command {
                fn name() -> &'static str {$name}
                fn help() -> &'static str {$help}
            }
        )*
        fn _command_help(name: &str) -> Result<&str> {
            match name {
                $($name => Ok($command::help()), )*
                _ => fail!("command {} not supported", name)
            }
        }
        fn command_send<Q: ToString>(name: &str, params: impl Iterator<Item = Q>) -> Result<TLObject> {
            match name {
                $($name => $command::send(params), )*
                _ => fail!("command {} not supported", name)
            }
        }
        fn command_receive<Q: ToString>(
            name: &str,
            answer: TLObject,
            params: impl Iterator<Item = Q>
        ) -> Result<(String, Vec<u8>)> {
            match name {
                $($name => $command::receive(answer, params), )*
                _ => fail!("an error occured while receiving a response (command: {})", name)
            }
        }
    };
}

commands! {
    AddAdnlAddr, "addadnl", "addadnl <keyhash> <category>\tuse key as ADNL addr"
    AddValidatorAdnlAddr, "addvalidatoraddr", "addvalidatoraddr <permkeyhash> <keyhash> <expireat>\tadd validator ADNL addr"
    AddValidatorPermKey, "addpermkey", "addpermkey <keyhash> <election-date> <expire-at>\tadd validator permanent key"
    AddValidatorTempKey, "addtempkey", "addtempkey <permkeyhash> <keyhash> <expire-at>\tadd validator temp key"
    Bundle, "bundle", "bundle <block_id>\tprepare bundle"
    ExportPub, "exportpub", "exportpub <keyhash>\texports public key by key hash"
    FutureBundle, "future_bundle", "future_bundle <block_id>\tprepare future bundle"
    GetAccount, "getaccount", "getaccount <account id> <Option<file name>>\tget account info"
    GetAccountState, "getaccountstate", "getaccountstate <account id> <file name>\tsave accountstate to file"
    GetBlockchainConfig, "getblockchainconfig", "getblockchainconfig\tget current config from masterchain state"
    GetConfig, "getconfig", "getconfig <param_number>\tget current config param from masterchain state"
    GetSessionStats, "getconsensusstats", "getconsensusstats\tget consensus statistics for the node"
    GetSelectedStats, "getstatsnew", "getstatsnew\tget status full node or validator in new format"
    GetStats, "getstats", "getstats\tget status full node or validator"
    NewKeypair, "newkey", "newkey\tgenerates new key pair on server"
    SendMessage, "sendmessage", "sendmessage <filename>\tload a serialized message from <filename> and send it to server"
    SetStatesGcInterval, "setstatesgcinterval", "setstatesgcinterval <milliseconds>\tset interval in <milliseconds> between shard states GC runs"
    Sign, "sign", "sign <keyhash> <data>\tsigns bytestring with privkey"
}

fn parse_any<A, Q: ToString>(param_opt: Option<Q>, name: &str, parse_value: impl FnOnce(&str) -> Result<A>) -> Result<A> {
    param_opt
        .ok_or_else(|| error!("insufficient parameters"))
        .and_then(|value| parse_value(value.to_string().trim_matches('\"')))
        .map_err(|err| error!("you must give {}: {}", name, err))
}

fn downcast<T: ton_api::AnyBoxedSerialize>(data: TLObject) -> Result<T> {
    match data.downcast::<T>() {
        Ok(result) => Ok(result),
        Err(obj) => fail!("Wrong downcast {:?} to {}", obj, std::any::type_name::<T>())
    }
}

fn parse_data<Q: ToString>(param_opt: Option<Q>, name: &str) -> Result<ton::bytes> {
    parse_any(
        param_opt,
        &format!("{} in hex format", name),
        |value| Ok(hex::decode(value)?)
    )
}

fn parse_int256<Q: ToString>(param_opt: Option<Q>, name: &str) -> Result<UInt256> {
    parse_any(
        param_opt,
        &format!("{} in hex or base64 format", name),
        |value| {
            let value = match value.len() {
                44 => base64_decode(value)?,
                64 => hex::decode(value)?,
                length => fail!("wrong hash: {} with length: {}", value, length)
            };
            Ok(UInt256::with_array(value.as_slice().try_into()?))
        }
    )
}

fn parse_int<Q: ToString>(param_opt: Option<Q>, name: &str) -> Result<ton::int> {
    parse_any(param_opt, name, |value| Ok(ton::int::from_str(value)?))
}

fn parse_blockid<Q: ToString>(param_opt: Option<Q>, name: &str) -> Result<BlockIdExt> {
    parse_any(param_opt, name, |value| BlockIdExt::from_str(value))
}

fn now() -> ton::int {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as ton::int
}

fn stats_to_json<'a>(stats: impl IntoIterator<Item = &'a OneStat>) -> serde_json::Value {
    let map = stats.into_iter().map(|stat| {
        let value = if stat.value.is_empty() {
            "null".into()
        } else if let Ok(value) = stat.value.parse::<i64>() {
            value.into()
        } else if let Ok(value) = serde_json::from_str(&stat.value) {
            value
        } else {
            stat.value.trim_matches('\"').into()
        };
        (stat.key.clone(), value)
    }).collect::<serde_json::Map<_, _>>();
    map.into()
}

impl SendReceive for GetStats {
    fn send<Q: ToString>(_params: impl Iterator<Item = Q>) -> Result<TLObject> {
        Ok(TLObject::new(ton::rpc::engine::validator::GetStats))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let data = serialize_boxed(&answer)?;
        let stats = downcast::<ton_api::ton::engine::validator::Stats>(answer)?;
        let description = stats_to_json(stats.stats().iter());
        let description = format!("{:#}", description);
        Ok((description, data))
    }
}

impl SendReceive for GetSelectedStats {
    fn send<Q: ToString>(_params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let req = ton::rpc::engine::validator::GetSelectedStats {
            filter: "*".to_string()
        };
        Ok(TLObject::new(req))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let data = serialize_boxed(&answer)?;
        let stats = downcast::<ton_api::ton::engine::validator::Stats>(answer)?;
        let description = stats_to_json(stats.stats().iter());
        let description = format!("{:#}", description);
        Ok((description, data))
    }
}

impl SendReceive for GetSessionStats {
    fn send<Q: ToString>(_params: impl Iterator<Item = Q>) -> Result<TLObject> {
        Ok(TLObject::new(ton::rpc::engine::validator::GetSessionStats))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let data = serialize_boxed(&answer)?;
        let stats = downcast::<ton_api::ton::engine::validator::SessionStats>(answer)?;
        let description = stats.stats().iter().map(|session_stat| {
            (session_stat.session_id.clone(), stats_to_json(session_stat.stats.iter()))
        }).collect::<serde_json::Map<_, _>>();
        let description = format!("{:#}", serde_json::Value::from(description));
        Ok((description, data))
    }
}

impl SendReceive for NewKeypair {
    fn send<Q: ToString>(_params: impl Iterator<Item = Q>) -> Result<TLObject> {
        Ok(TLObject::new(ton::rpc::engine::validator::GenerateKeyPair))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let answer = downcast::<ton_api::ton::engine::validator::KeyHash>(answer)?;
        let key_hash = answer.key_hash().as_slice().to_vec();
        let msg = format!(
            "received public key hash: {} {}", 
            hex::encode(&key_hash), base64_encode(&key_hash)
        );
        Ok((msg, key_hash))
    }
}

impl SendReceive for ExportPub {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let key_hash = parse_int256(params.next(), "key_hash")?;
        Ok(TLObject::new(ton::rpc::engine::validator::ExportPublicKey {
            key_hash
        }))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let answer = downcast::<ton_api::ton::PublicKey>(answer)?;
        let pub_key = answer
            .key()
            .ok_or_else(|| error!("Public key not found in answer!"))?
            .as_slice()
            .to_vec();
        let msg = format!("imported key: {} {}", hex::encode(&pub_key), base64_encode(&pub_key));
        Ok((msg, pub_key))
    }
}

impl SendReceive for Sign {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let key_hash = parse_int256(params.next(), "key_hash")?;
        let data = parse_data(params.next(), "data")?;
        Ok(TLObject::new(ton::rpc::engine::validator::Sign {
            key_hash,
            data
        }))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let answer = downcast::<ton_api::ton::engine::validator::Signature>(answer)?;
        let signature = answer.signature().clone();
        let msg = format!(
            "got signature: {} {}", 
            hex::encode(&signature), base64_encode(&signature)
        );
        Ok((msg, signature))
    }
}

impl SendReceive for AddValidatorPermKey {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let key_hash =  parse_int256(params.next(), "key_hash")?;
        let election_date = parse_int(params.next(), "election_date")?;
        let ttl = parse_int(params.next(), "expire_at")? - election_date;
        Ok(TLObject::new(ton::rpc::engine::validator::AddValidatorPermanentKey {
            key_hash,
            election_date,
            ttl
        }))
    }
}

impl SendReceive for AddValidatorTempKey {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let permanent_key_hash = parse_int256(params.next(), "permanent_key_hash")?;
        let key_hash = parse_int256(params.next(), "key_hash")?;
        let ttl = parse_int(params.next(), "expire_at")? - now();
        Ok(TLObject::new(ton::rpc::engine::validator::AddValidatorTempKey {
            permanent_key_hash,
            key_hash,
            ttl
        }))
    }
}

impl SendReceive for AddValidatorAdnlAddr {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let permanent_key_hash = parse_int256(params.next(), "permanent_key_hash")?;
        let key_hash = parse_int256(params.next(), "key_hash")?;
        let ttl = parse_int(params.next(), "expire_at")? - now();
        Ok(TLObject::new(ton::rpc::engine::validator::AddValidatorAdnlAddress {
            permanent_key_hash,
            key_hash,
            ttl
        }))
    }
}

impl SendReceive for AddAdnlAddr {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let key_hash = parse_int256(params.next(), "key_hash")?;
        let category = parse_int(params.next(), "category")?;
        if category < 0 || category > 15 {
            fail!("category must be not negative and less than 16")
        }
        Ok(TLObject::new(ton::rpc::engine::validator::AddAdnlId {
            key_hash,
            category
        }))
    }
}

impl SendReceive for Bundle {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let block_id = parse_blockid(params.next(), "block_id")?;
        Ok(TLObject::new(ton::rpc::engine::validator::GetBundle {
            block_id
        }))
    }
}

impl SendReceive for FutureBundle {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let mut prev_block_ids = vec![parse_blockid(params.next(), "block_id")?];
        if let Ok(block_id) = parse_blockid(params.next(), "block_id") {
            prev_block_ids.push(block_id);
        }
        Ok(TLObject::new(ton::rpc::engine::validator::GetFutureBundle {
            prev_block_ids: prev_block_ids.into()
        }))
    }
}

impl SendReceive for SendMessage {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let filename = params.next().ok_or_else(|| error!("insufficient parameters"))?.to_string();
        let body = std::fs::read(&filename)
            .map_err(|e| error!("Can't read file {} with message: {}", filename, e))?;
        Ok(TLObject::new(ton::rpc::lite_server::SendMessage {body: body.into()}))
    }
}

impl SendReceive for GetBlockchainConfig {
    fn send<Q: ToString>(_params: impl Iterator<Item = Q>) -> Result<TLObject> {
        Ok(TLObject::new(ton::rpc::lite_server::GetConfigAll {
            mode: 0,
            id: BlockIdExt::default()
        }))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let config_info = downcast::<ton_api::ton::lite_server::ConfigInfo>(answer)?;

        // We use config_proof because we use standard struct ConfigInfo from ton-tl and
        // ConfigInfo doesn`t contain more suitable fields
        let config_param = hex::encode(config_info.config_proof().clone());
        Ok((format!("{}", config_param), config_info.config_proof().clone()))
    }
}

impl SendReceive for GetConfig {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let param_number = parse_int(params.next(), "paramnumber")?;
        let mut params: ton::vector<ton::Bare, ton::int> = ton::vector::default();
        params.0.push(param_number);
        Ok(TLObject::new(ton::rpc::lite_server::GetConfigParams {
            mode: 0,
            id: BlockIdExt::default(),
            param_list: params
        }))
    }
    fn receive<Q: ToString>(
        answer: TLObject, 
        mut _params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let config_info = downcast::<ton_api::ton::lite_server::ConfigInfo>(answer)?;
        let config_param = String::from_utf8(config_info.config_proof().clone())?;
        Ok((config_param.to_string(), config_info.config_proof().clone()))
    }
}

impl SendReceive for GetAccount {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let account = AccountAddress { 
            account_address: params.next().ok_or_else(|| error!("insufficient parameters"))?.to_string()
        };
        Ok(TLObject::new(ton::rpc::raw::GetShardAccountState {account_address: account}))
    }

    fn receive<Q: ToString>(
        answer: TLObject, 
        mut params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let shard_account_state = downcast::<ShardAccountState>(answer)?;
        let mut account_info = String::from("{");
        account_info.push_str("\n\"");
        account_info.push_str("acc_type\":\t\"");

        match shard_account_state {
            ShardAccountState::Raw_ShardAccountNone => {
                account_info.push_str(&"Nonexist");
            },
            ShardAccountState::Raw_ShardAccountState(account_state) => {
                let shard_account = ShardAccount::construct_from_bytes(&account_state.shard_account)?;
                let account = shard_account.read_account()?;

                let account_type = match account.status() {
                    AccountStatus::AccStateUninit => "Uninit",
                    AccountStatus::AccStateFrozen => "Frozen",
                    AccountStatus::AccStateActive => "Active",
                    AccountStatus::AccStateNonexist => "Nonexist"
                };
                let balance = account.balance().map_or(0, |val| val.grams.as_u128());
                account_info.push_str(&account_type);
                account_info.push_str("\",\n\"");
                account_info.push_str("balance\":\t");
                account_info.push_str(&balance.to_string());
                account_info.push_str(",\n\"");
                account_info.push_str("last_paid\":\t");
                account_info.push_str(&account.last_paid().to_string());
                account_info.push_str(",\n\"");
                account_info.push_str("last_trans_lt\":\t\"");
                account_info.push_str(&format!("{:#x}", shard_account.last_trans_lt()));
                account_info.push_str("\",\n\"");
                account_info.push_str("data(boc)\":\t\"");
                account_info.push_str(
                    &hex::encode(&write_boc(&shard_account.account_cell())?)
                );
            }
        }
        account_info.push_str("\"\n}");

        params.next();
        let account_data = account_info.as_bytes().to_vec();
        if let Some(boc_name) = params.next() {
            std::fs::write(boc_name.to_string(), &account_data)
                .map_err(|err| error!("Can`t create file: {}", err))?;
        }

        Ok((account_info, account_data))
    }
}

impl SendReceive for GetAccountState {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let account_address = params.next().ok_or_else(|| error!("insufficient parameters"))?.to_string();
        let account_address = AccountAddress { account_address };
        Ok(TLObject::new(ton::rpc::raw::GetShardAccountState {account_address}))
    }

    fn receive<Q: ToString>(
        answer: TLObject, 
        mut params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let shard_account_state = downcast::<ShardAccountState>(answer)?;

        params.next();
        let boc_name = params
            .next()
            .ok_or_else(|| error!("bad params (boc name not found)!"))?
            .to_string();

        let shard_account_state = shard_account_state
            .shard_account()
            .ok_or_else(|| error!("account not found!"))?;
        
        let shard_account = ShardAccount::construct_from_bytes(&shard_account_state)?;
        let account_state = write_boc(&shard_account.account_cell())?;
        std::fs::write(boc_name, &account_state)
            .map_err(|err| error!("Can`t create file: {}", err))?;

        Ok((format!("{} {}",
            hex::encode(&account_state),
            base64_encode(&account_state)),
            account_state)
        )
    }
}

impl SendReceive for SetStatesGcInterval {
    fn send<Q: ToString>(mut params: impl Iterator<Item = Q>) -> Result<TLObject> {
        let interval_ms_str = params.next().ok_or_else(|| error!("insufficient parameters"))?.to_string();
        let interval_ms = interval_ms_str.parse().map_err(|e| error!("can't parse <milliseconds>: {}", e))?;
        Ok(TLObject::new(ton::rpc::engine::validator::SetStatesGcInterval { interval_ms }))
    }
}

/// ControlClient
struct ControlClient{
    config: AdnlConsoleConfigJson,
    adnl: AdnlClient,
}

impl ControlClient {

    /// Connect to server
    async fn connect(mut config: AdnlConsoleConfigJson) -> Result<Self> {
        let client_config = config.config.take()
            .ok_or_else(|| error!("config must contain \"config\" section"))?;
        let (_, adnl_config) = AdnlClientConfig::from_json_config(client_config)?;
        Ok(Self {
            config,
            adnl: AdnlClient::connect(&adnl_config).await?,
        })
    }

    /// Shutdown client
    async fn shutdown(self) -> Result<()> {
        self.adnl.shutdown().await
    }

    async fn command(&mut self, cmd: &str) -> Result<(String, Vec<u8>)> {
        let result = shell_words::split(cmd)?;
        let mut params = result.iter();
        match params.next().expect("takes_value set for COMMANDS").as_str() {
            "recover_stake" => self.process_recover_stake(params).await,
            "ebid" |
            "election-bid" |
            "election_bid" => self.process_election_bid(params).await,
            "config_param" |
            "cparam" => self.process_config_param(params).await,
            name => self.process_command(name, params).await
        }
    }

    async fn process_command<Q: ToString>(
        &mut self,
        name: &str,
        params: impl Iterator<Item = Q> + Clone
    ) -> Result<(String, Vec<u8>)> {
        let query = command_send(name, params.clone())?;
        let boxed = ControlQuery {
            data: serialize_boxed(&query)?
        };
        #[cfg(feature = "telemetry")]
        let tag = tag_from_bare_object(&boxed);
        let boxed = TaggedTlObject {
            object: TLObject::new(boxed),
            #[cfg(feature = "telemetry")]
            tag
        };
        let answer = self.adnl.query(&boxed).await
            .map_err(|err| error!("Error receiving answer: {}", err))?;
        match answer.downcast::<ControlQueryError>() {
            Err(answer) => match command_receive(name, answer, params) {
                Err(answer) => fail!("Wrong response to {:?}: {:?}", query, answer),
                Ok(result) => Ok(result)
            }
            Ok(error) => fail!("Error response to {:?}: {:?}", query, error),
        }
    }

    async fn process_recover_stake<Q: ToString>(
        &mut self, 
        mut params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let query_id = now() as u64;
        // recover-stake.fif
        let mut data = 0x47657424u32.to_be_bytes().to_vec();
        data.extend_from_slice(&query_id.to_be_bytes());
        let len = data.len() * 8;
        let body = BuilderData::with_raw(data, len)?;
        let body = body.into_cell()?;
        log::trace!("message body {}", body);
        let data = ton_types::write_boc(&body)?;
        let path = params.next().map(
            |path| path.to_string()).unwrap_or("recover-query.boc".to_string()
        );
        std::fs::write(&path, &data)?;
        Ok((format!("Message body is {} saved to path {}", base64_encode(&data), path), data))
    }

    // @input elect_time expire_time <validator-query.boc>
    // @output validator-query.boc
    async fn process_election_bid<Q: ToString>(
        &mut self, 
        mut params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let wallet_id = parse_any(self.config.wallet_id.as_ref(), "wallet_id", |value| {
            match value.strip_prefix("-1:") {
                Some(stripped) => Ok(hex::decode(stripped)?),
                None => fail!("use masterchain wallet")
            }
        })?;
        let elect_time = parse_int(params.next(), "elect_time")?;
        if elect_time <= 0 {
            fail!("<elect-utime> must be a positive integer")
        }
        let elect_time_str = elect_time.to_string();
        let expire_time = parse_int(params.next(), "expire_time")?;
        if expire_time <= elect_time {
            fail!("<expire-utime> must be a grater than elect_time")
        }
        let expire_time_str = expire_time.to_string();
        let max_factor = self.config.max_factor.ok_or_else(
            || error!("you must give max_factor as real")
        )?;
        if max_factor < 1.0 || max_factor > 100.0 {
            fail!("<max-factor> must be a real number 1..100")
        }
        let max_factor = (max_factor * 65536.0) as u32;

        let (s, perm) = self.process_command("newkey", Vec::<String>::new().iter()).await?;
        log::trace!("{}", s);
        let perm_str = hex::encode_upper(&perm);

        let (s, pub_key) = self.process_command("exportpub", [&perm_str].iter()).await?;
        log::trace!("{}", s);

        let (s, _) = self.process_command(
            "addpermkey", 
            [&perm_str, &elect_time_str, &expire_time_str].iter()
        ).await?;
        log::trace!("{}", s);

        let (s, _) = self.process_command(
            "addtempkey", 
            [&perm_str, &perm_str, &expire_time_str].iter()
        ).await?;
        log::trace!("{}", s);

        let (s, adnl) = self.process_command("newkey", Vec::<String>::new().iter()).await?;
        log::trace!("{}", s);
        let adnl_str = hex::encode_upper(&adnl);

        let (s, _) = self.process_command("addadnl", [&adnl_str, "0"].iter()).await?;
        log::trace!("{}", s);

        let (s, _) = self.process_command(
            "addvalidatoraddr", 
            [&perm_str, &adnl_str, &elect_time_str].iter()
        ).await?;
        log::trace!("{}", s);

        // validator-elect-req.fif
        let mut data = 0x654C5074u32.to_be_bytes().to_vec();
        data.extend_from_slice(&elect_time.to_be_bytes());
        data.extend_from_slice(&max_factor.to_be_bytes());
        data.extend_from_slice(&wallet_id);
        data.extend_from_slice(&adnl);
        let data_str = hex::encode_upper(&data);
        log::trace!("data to sign {}", data_str);
        let (s, signature) = self.process_command("sign", [&perm_str, &data_str].iter()).await?;
        log::trace!("{}", s);
        Ed25519KeyOption::from_public_key(&pub_key[..].try_into()?)
            .verify(&data, &signature)?;

        let query_id = now() as u64;
        // validator-elect-signed.fif
        let mut data = 0x4E73744Bu32.to_be_bytes().to_vec();
        data.extend_from_slice(&query_id.to_be_bytes());
        data.extend_from_slice(&pub_key);
        data.extend_from_slice(&elect_time.to_be_bytes());
        data.extend_from_slice(&max_factor.to_be_bytes());
        data.extend_from_slice(&adnl);
        let len = data.len() * 8;
        let mut body = BuilderData::with_raw(data, len)?;
        let len = signature.len() * 8;
        body.checked_append_reference(BuilderData::with_raw(signature, len)?.into_cell()?)?;
        let body = body.into_cell()?;
        log::trace!("message body {}", body);
        let data = ton_types::write_boc(&body)?;
        let path = params.next().map(
            |path| path.to_string()).unwrap_or("validator-query.boc".to_string()
        );
        std::fs::write(&path, &data)?;
        Ok((format!("Message body is {} saved to path {}", base64_encode(&data), path), data))
    }

    // @input index zerostate.json <config-param.boc>
    // @output config-param.boc
    async fn process_config_param<Q: ToString>(
        &mut self, 
        mut params: impl Iterator<Item = Q>
    ) -> Result<(String, Vec<u8>)> {
        let index = parse_int(params.next(), "index")?;
        if index < 0 {
            fail!("<index> must not be a negative integer")
        }
        let zerostate = parse_any(params.next(), "zerostate.json", |value| Ok(value.to_string()))?;
        let path = params.next().map(
            |path| path.to_string()
        ).unwrap_or("config-param.boc".to_string());

        let zerostate = std::fs::read_to_string(&zerostate)
            .map_err(|err| error!("Can't read zerostate json file {} : {}", zerostate, err))?;
        let zerostate = 
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&zerostate)
                .map_err(|err| error!("Can't parse read zerostate json file: {}", err))?;
        let zerostate = ton_block_json::parse_state(&zerostate)
            .map_err(|err| error!("Can't parse read zerostate json file: {}", err))?;

        let key = SliceData::load_builder(index.write_to_new_cell()?)?;
        let config_param_cell = zerostate.read_custom()
            .map_err(|err| error!("Can't read McStateExtra from zerostate: {}", err))?
            .ok_or_else(|| error!("Can't find McStateExtra in zerostate"))?
            .config().config_params.get(key)
            .map_err(|err| error!("Can't read config param {} from zerostate: {}", index, err))?
            .ok_or_else(|| error!("Can't find config param {} in zerostate", index))?
            .reference_opt(0)
            .ok_or_else(|| error!("Can't parse config param {}: wrong format - no reference", index))?;

        let data = write_boc(&config_param_cell)
            .map_err(|err| error!("Can't serialize config param {}: {}", index, err))?;

        std::fs::write(&path, &data)
            .map_err(|err| error!("Can't write config param {} to file {}: {}", index, path, err))?;

        Ok((format!("Config param {} saved to path {}", index, path), data))
    }
}

#[derive(serde::Deserialize)]
struct AdnlConsoleConfigJson {
    config: Option<AdnlClientConfigJson>,
    wallet_id: Option<String>,
    max_factor: Option<f32>
}

#[tokio::main]
async fn main() {
    // init_test_log();
    let args = clap::App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(clap::Arg::with_name("CONFIG")
            .short("C")
            .long("config")
            .help("config for console")
            .required(true)
            .default_value("console.json")
            .takes_value(true)
            .number_of_values(1))
        .arg(clap::Arg::with_name("COMMANDS")
            .allow_hyphen_values(true)
            .short("c")
            .long("cmd")
            .help("schedule command")
            .multiple(true)
            .takes_value(true))
        .arg(clap::Arg::with_name("TIMEOUT")
            .short("t")
            .long("timeout")
            .help("timeout in batch mode")
            .takes_value(true)
            .number_of_values(1))
        .arg(clap::Arg::with_name("VERBOSE")
            .long("verbose")
            .help("verbose regim"))
        .arg(clap::Arg::with_name("JSON")
            .short("j")
            .long("json")
            .help("output in json format")
            .takes_value(false))
        .get_matches();

    if !args.is_present("JSON") {
        println!(
            "tonlabs console {}\nCOMMIT_ID: {}\nBUILD_DATE: {}\nCOMMIT_DATE: {}\nGIT_BRANCH: {}",
            env!("CARGO_PKG_VERSION"),
            env!("BUILD_GIT_COMMIT"),
            env!("BUILD_TIME") ,
            env!("BUILD_GIT_DATE"),
            env!("BUILD_GIT_BRANCH")
        );
    }

    if args.is_present("VERBOSE") {
        let encoder_boxed = Box::new(log4rs::encode::pattern::PatternEncoder::new("{m}{n}"));
        let console = log4rs::append::console::ConsoleAppender::builder()
            .encoder(encoder_boxed)
            .build();
        let config = log4rs::config::Config::builder()
            .appender(log4rs::config::Appender::builder().build("console", Box::new(console)))
            .build(log4rs::config::Root::builder().appender("console").build(log::LevelFilter::Trace))
            .unwrap();
        log4rs::init_config(config).unwrap();
    }

    let config = args.value_of("CONFIG").expect("required set for config");
    let config = std::fs::read_to_string(config).expect("Can't read config file");
    let config = serde_json::from_str(&config).expect("Can't parse config");
    let timeout = match args.value_of("TIMEOUT") {
        Some(timeout) => u64::from_str(timeout).expect("timeout must be set in microseconds"),
        None => 0
    };
    let timeout = Duration::from_micros(timeout);
    let mut client = ControlClient::connect(config).await.expect("Can't create client");
    if let Some(commands) = args.values_of("COMMANDS") {
        // batch mode - call commands and exit
        for command in commands {
            match client.command(command.trim_matches('\"')).await {
                Ok((result, _)) => println!("{}", result),
                Err(err) => println!("Error executing command: {}", err)
            }
            tokio::time::sleep(timeout).await;
        }
    } else {
        // interactive mode
        loop {
            let mut line = String::default();
            std::io::stdin().read_line(&mut line).expect("Can't read line from stdin");
            match line.trim_end() {
                "" => continue,
                "quit" => break,
                command => match client.command(command).await {
                    Ok((result, _)) => println!("{}", result),
                    Err(err) => println!("{}", err)
                }
            }
        }
    }
    client.shutdown().await.ok();
}

#[cfg(test)]
mod test {
    
    use super::*;
    use std::{fs, path::Path, sync::Arc, thread};
    use serde_json::json;
    use storage::block_handle_db::BlockHandle;
    use ton_api::deserialize_boxed;
    use ton_block::{
        generate_test_account_by_init_code_hash,
        BlockLimits, ConfigParam0, ConfigParam34, ConfigParamEnum, McStateExtra, ParamLimits, 
        ShardIdent, ShardStateUnsplit, ValidatorDescr, ValidatorSet
    };
    use ton_node::{
        collator_test_bundle::{create_engine_telemetry, create_engine_allocated},
        config::TonNodeConfig, engine_traits::{EngineAlloc, EngineOperations},
        internal_db::{InternalDbConfig, InternalDb, state_gc_resolver::AllowStateGcSmartResolver}, 
        network::{control::{ControlServer, DataSource}, node_network::NodeNetwork},
        shard_state::ShardStateStuff,
        validator::validator_manager::ValidationStatus, shard_states_keeper::PinnedShardStateGuard,
    };
    #[cfg(feature = "telemetry")]
    use ton_node::engine_traits::EngineTelemetry;

    const CFG_DIR: &str = "./target";
    const CFG_NODE_FILE: &str = "light_node.json";
    const CFG_GLOB_FILE: &str = "light_global.json";
    const DB_PATH: &str = "./target/node_db";

    struct TestEngine {
        db: InternalDb,
        master_state: Arc<ShardStateStuff>,
        master_state_id: BlockIdExt,
        shard_state: Arc<ShardStateStuff>,
        shard_state_id: BlockIdExt,
        last_validation_time: lockfree::map::Map<ShardIdent, u64>
    }

    impl TestEngine {

        async fn new(
            #[cfg(feature = "telemetry")]
            telemetry: Arc<EngineTelemetry>,
            allocated: Arc<EngineAlloc>
        ) -> Self {

            let mut ss = ShardStateUnsplit::with_ident(ShardIdent::full(0));
            let account = generate_test_account_by_init_code_hash(false);
            let account_id = account.get_id().unwrap().get_next_hash().unwrap();
            ss.insert_account(
                &account_id, 
                &ShardAccount::with_params(&account, UInt256::default(), 0).unwrap()
            ).unwrap();
            let cell = ss.serialize().unwrap();
            let bytes = ton_types::write_boc(&cell).unwrap();
            let shard_state_id = BlockIdExt::with_params(
                ShardIdent::full(0),
                0,
                cell.repr_hash(),
                UInt256::calc_file_hash(&bytes)
            );
            let shard_state = ShardStateStuff::deserialize_zerostate(
                shard_state_id.clone(), 
                &bytes,
                #[cfg(feature = "telemetry")]
                &telemetry,
                &allocated
            ).unwrap();

            let mut ss = ShardStateUnsplit::with_ident(ShardIdent::masterchain());
            let mut ms = McStateExtra::default();
            let mut param = ConfigParam0::new();
            param.config_addr = UInt256::from([1;32]);
            ms.config.set_config(ConfigParamEnum::ConfigParam0(param)).unwrap();
            let mut param = ConfigParam34::new();
            param.cur_validators = ValidatorSet::new(
                1600000000,
                1610000000,
                1,
                vec![ValidatorDescr::default()]
            ).unwrap();
            ms.config.set_config(ConfigParamEnum::ConfigParam34(param)).unwrap();
            ms.shards.add_workchain(
                0, 
                0,
                shard_state_id.root_hash.clone(),
                shard_state_id.file_hash.clone(),
                None
            ).unwrap();
            ss.write_custom(Some(&ms)).unwrap();

            let cell = ss.serialize().unwrap();
            let bytes = ton_types::write_boc(&cell).unwrap();
            let master_state_id = BlockIdExt::with_params(
                ShardIdent::masterchain(),
                0,
                cell.repr_hash(),
                UInt256::calc_file_hash(&bytes)
            );
            let master_state = ShardStateStuff::deserialize_zerostate(
                master_state_id.clone(), 
                &bytes,
                #[cfg(feature = "telemetry")]
                &telemetry,
                &allocated
            ).unwrap();

            fs::remove_dir_all(DB_PATH).ok();
            let db_config = InternalDbConfig {
                db_directory: String::from(DB_PATH),
                ..Default::default()
            };
            let db = InternalDb::with_update(
                db_config, 
                false,
                false,
                false,
                &|| Ok(()),
                None,
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone()
            ).await.unwrap();
            db.create_or_load_block_handle(
                &master_state_id,
                None,
                None,
                Some(1),
                None
            ).unwrap()._to_created().unwrap();

            Self {
                db, 
                master_state, 
                master_state_id,
                shard_state, 
                shard_state_id,
                last_validation_time: lockfree::map::Map::new()
            }

        }

        async fn stop(&self) {
            self.db.stop_states_db().await    
        }

    }

    impl Drop for TestEngine {
        fn drop(&mut self) {
            fs::remove_file(Path::new(CFG_DIR).join(CFG_NODE_FILE)).ok();
            fs::remove_file(Path::new(CFG_DIR).join(CFG_GLOB_FILE)).ok();         
        }
    }

    #[async_trait::async_trait]
    impl EngineOperations for TestEngine {
        fn calc_tps(&self, _period: u64) -> Result<u32> {
            Ok(0)
        }
        fn validation_status(&self) -> ValidationStatus {
            ValidationStatus::Active
        }
        fn last_validation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
            &self.last_validation_time
        }
        fn last_collation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
            &self.last_validation_time
        }
        fn get_sync_status(&self) -> u32 {
            0
        }
        fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
            self.db.load_block_handle(id)
        }
        fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.master_state_id.clone())))
        }
        async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
            Ok(self.master_state.clone())   
        }
        fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.master_state_id.clone())))
        }
        async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
            if block_id == &self.master_state_id {
                Ok(self.master_state.clone())   
            } else if block_id == &self.shard_state_id {
                Ok(self.shard_state.clone())   
            } else {
                fail!("Wrong block ID {}", block_id)
            }
        }
        async fn load_and_pin_state(&self, block_id: &BlockIdExt) -> Result<PinnedShardStateGuard> {
            if *block_id == self.master_state_id {
                PinnedShardStateGuard::new(
                    self.master_state.clone(),
                    Arc::new(AllowStateGcSmartResolver::new(10))
                )
            } else if *block_id == self.shard_state_id {
                PinnedShardStateGuard::new(
                    self.shard_state.clone(),
                    Arc::new(AllowStateGcSmartResolver::new(10))
                )
            } else {
                fail!("Wrong block ID {}", block_id)
            }
        }
        fn find_full_block_id(&self, root_hash: &UInt256) -> Result<Option<BlockIdExt>> {
            Ok(if *root_hash == self.master_state_id.root_hash {
                Some(self.master_state_id.clone())
            } else if *root_hash == self.shard_state_id.root_hash {
                Some(self.shard_state_id.clone())
            } else {
                None
            })
        }
    }
    
    const ADNL_SERVER_CONFIG: &str = r#"{
        "ton_global_config_name": "light_global.json",
        "adnl_node": {
            "ip_address": "127.0.0.1:4191",
            "keys": [
                {
                    "tag": 1,
                    "data": {
                        "type_id": 1209251014,
                        "pvt_key": "x3osWUUfcdybRLcmH7mJer4S0NM7TDpEw4SCftKD7bY="
                    }
                },
                {
                    "tag": 2,
                    "data": {
                        "type_id": 1209251014,
                        "pvt_key": "K56upUuRmuZrXhuExmmgr3DQt5Ae12XXQDXUXeGCQxg="
                    }
                }
            ]
        },
        "control_server": {
            "address": "127.0.0.1:4924",
            "server_key": {
                "type_id": 1209251014,
                "pvt_key": "cJIxGZviebMQWL726DRejqVzRTSXPv/1sO/ab6XOZXk="
            },
            "clients": {
                "list": [
                    {
                        "type_id": 1209251014,
                        "pub_key": "RYokIiD5AFkzfTBgC6NhtAGFKm0+gwhN4suTzaW0Sjw="
                    }
                ]
            }
        }
    }"#;

    const ADNL_CLIENT_CONFIG: &str = r#"{
        "config": {
            "server_address": "127.0.0.1:4924",
            "server_key": {
                "type_id": 1209251014,
                "pub_key": "cujCRU4rQbSw48yHVHxQtRPhUlbo+BuZggFTQSu04Y8="
            },
            "client_key": {
                "type_id": 1209251014,
                "pvt_key": "oEivbTDjSOSCgooUM0DAS2z2hIdnLw/PT82A/OFLDmA="
            }
        },
        "wallet_id": "-1:af17db43f40b6aa24e7203a9f8c8652310c88c125062d1129fe883eaa1bd6763",
        "max_factor": 2.7
    }"#;

    const GLOBAL_CONFIG: &str = r#"{
        "validator": {
            "zero_state": {
                "workchain": -1,
                "shard": -9223372036854775808,
                "seqno": 0,
                "root_hash": "6Jb3GskUbO+HxaCnKQPEDPSxCkck7zHL//xPwAmBql0=",
                "file_hash": "Ti5G+UTHu6MTe2IVmMZ7VcQz+dBpET+NApZm+0B4kfc="
            }
        }
    }"#;

    const SAMPLE_ZERO_STATE: &str = r#"{
        "id": "-1:8000000000000000",
        "workchain_id": -1,
        "boc": "",
        "global_id": 42,
        "shard": "8000000000000000",
        "seq_no": 0,
        "vert_seq_no": 0,
        "gen_utime": 1600000000,
        "gen_lt": "0",
        "min_ref_mc_seqno": 4294967295,
        "before_split": false,
        "overload_history": "0",
        "underload_history": "0",
        "total_balance": "4993357197000000000",
        "total_validator_fees": "0",
        "master": {
            "config_addr": "5555555555555555555555555555555555555555555555555555555555555555",
            "config": {
            "p0": "5555555555555555555555555555555555555555555555555555555555555555",
            "p1": "3333333333333333333333333333333333333333333333333333333333333333",
            "p2": "0000000000000000000000000000000000000000000000000000000000000000",
            "p7": [],
            "p8": {
                "version": 5,
                "capabilities": "46"
            },
            "p9": [ 0 ],
            "p10": [ 0 ],
            "p11": {
                "normal_params": {
                    "min_tot_rounds": 2,
                    "max_tot_rounds": 3,
                    "min_wins": 2,
                    "max_losses": 2,
                    "min_store_sec": 1000000,
                    "max_store_sec": 10000000,
                    "bit_price": 1,
                    "cell_price": 500
                },
                "critical_params": {
                    "min_tot_rounds": 4,
                    "max_tot_rounds": 7,
                    "min_wins": 4,
                    "max_losses": 2,
                    "min_store_sec": 5000000,
                    "max_store_sec": 20000000,
                    "bit_price": 2,
                    "cell_price": 1000
                }
            },
            "p12": [],
            "p13": {
                "boc": "te6ccgEBAQEADQAAFRpRdIdugAEBIB9I"
            },
            "p14": {
                "masterchain_block_fee": "1700000000",
                "basechain_block_fee": "1000000000"
            },
            "p15": {
                "validators_elected_for": 65536,
                "elections_start_before": 32768,
                "elections_end_before": 8192,
                "stake_held_for": 32768
            },
            "p16": {
                "max_validators": 1000,
                "max_main_validators": 100,
                "min_validators": 5
            },
            "p17": {
                "min_stake": "10000000000000",
                "max_stake": "10000000000000000",
                "min_total_stake": "100000000000000",
                "max_stake_factor": 196608
            },
            "p18": [
                {
                "utime_since": 0,
                "bit_price_ps": "1",
                "cell_price_ps": "500",
                "mc_bit_price_ps": "1000",
                "mc_cell_price_ps": "500000"
                }
            ],
            "p20": {
                "flat_gas_limit": "1000",
                "flat_gas_price": "10000000",
                "gas_price": "655360000",
                "gas_limit": "1000000",
                "special_gas_limit": "100000000",
                "gas_credit": "10000",
                "block_gas_limit": "10000000",
                "freeze_due_limit": "100000000",
                "delete_due_limit": "1000000000"
            },
            "p21": {
                "flat_gas_limit": "1000",
                "flat_gas_price": "1000000",
                "gas_price": "65536000",
                "gas_limit": "1000000",
                "special_gas_limit": "1000000",
                "gas_credit": "10000",
                "block_gas_limit": "10000000",
                "freeze_due_limit": "100000000",
                "delete_due_limit": "1000000000"
            },
            "p22": {
                "bytes": {
                    "underload": 131072,
                    "soft_limit": 524288,
                    "hard_limit": 1048576
                },
                "gas": {
                    "underload": 900000,
                    "soft_limit": 1200000,
                    "hard_limit": 2000000
                },
                "lt_delta": {
                    "underload": 1000,
                    "soft_limit": 5000,
                    "hard_limit": 10000
                }
            },
            "p23": {
                "bytes": {
                    "underload": 131072,
                    "soft_limit": 524288,
                    "hard_limit": 1048576
                },
                "gas": {
                    "underload": 900000,
                    "soft_limit": 1200000,
                    "hard_limit": 2000000
                },
                "lt_delta": {
                    "underload": 1000,
                    "soft_limit": 5000,
                    "hard_limit": 10000
                }
            },
            "p24": {
                "lump_price": "10000000",
                "bit_price": "655360000",
                "cell_price": "65536000000",
                "ihr_price_factor": 98304,
                "first_frac": 21845,
                "next_frac": 21845
            },
            "p25": {
                "lump_price": "1000000",
                "bit_price": "65536000",
                "cell_price": "6553600000",
                "ihr_price_factor": 98304,
                "first_frac": 21845,
                "next_frac": 21845
            },
            "p28": {
                "shuffle_mc_validators": true,
                "mc_catchain_lifetime": 250,
                "shard_catchain_lifetime": 250,
                "shard_validators_lifetime": 1000,
                "shard_validators_num": 7
            },
            "p29": {
                "new_catchain_ids": true,
                "round_candidates": 3,
                "next_candidate_delay_ms": 2000,
                "consensus_timeout_ms": 16000,
                "fast_attempts": 3,
                "attempt_duration": 8,
                "catchain_max_deps": 4,
                "max_block_bytes": 2097152,
                "max_collated_bytes": 2097152
            },
            "p31": [
                "0000000000000000000000000000000000000000000000000000000000000000"
            ],
            "p34": {
                "utime_since": 1600000000,
                "utime_until": 1610000000,
                "total": 1,
                "main": 1,
                "total_weight": "17",
                "list": [
                    {
                        "public_key": "2e7eb5a711ed946605a91e36037c4cb927181eff4bb277b175d891a588d03536",
                        "weight": "17"
                    }
                ]
            }
            },
            "validator_list_hash_short": 871956759,
            "catchain_seqno": 0,
            "nx_cc_updated": true,
            "after_key_block": true,
            "global_balance": "4993357197000000000"
        },
        "accounts": [],
        "libraries": [],
        "out_msg_queue_info": {
            "out_queue": [],
            "proc_info": [],
            "ihr_pending": []
        }
    }"#;

    async fn init_test() -> (ControlServer, ControlClient, Arc<TestEngine>) {
        init_test_log();
        std::fs::write(Path::new(CFG_DIR).join(CFG_NODE_FILE), ADNL_SERVER_CONFIG).unwrap();
        std::fs::write(Path::new(CFG_DIR).join(CFG_GLOB_FILE), GLOBAL_CONFIG).unwrap();
        let node_config = TonNodeConfig::from_file(
            CFG_DIR, CFG_NODE_FILE, None, "", None
        ).unwrap();
        let control_server_config = node_config.control_server().unwrap();
        let config = control_server_config.expect("must have control server setting");
        #[cfg(feature = "telemetry")]
        let telemetry = create_engine_telemetry();
        let allocated = create_engine_allocated();
        let network = NodeNetwork::new(
            node_config,
            Arc::new(tokio_util::sync::CancellationToken::new()),
            #[cfg(feature = "telemetry")]
            telemetry.clone(),
            allocated.clone()
        ).await.unwrap();
        let engine = TestEngine::new(
            #[cfg(feature = "telemetry")]
            telemetry, 
            allocated
        ).await;
        let engine = Arc::new(engine);
        let server = ControlServer::with_params(
            config,
            DataSource::Engine(engine.clone()), 
            network.config_handler(),//.clone(), 
            network.config_handler(),//.clone(),
            Some(&network)//None
        ).await.unwrap();
        let config = serde_json::from_str(&ADNL_CLIENT_CONFIG).unwrap();
        let client = ControlClient::connect(config).await.unwrap();
        (server, client, engine)
    }

    async fn done_test(server: ControlServer, client: ControlClient, engine: Arc<TestEngine>) {
        server.shutdown().await;
        client.shutdown().await.ok();
        engine.stop().await;
        drop(engine);
        loop {
            if fs::remove_dir_all(DB_PATH).is_ok() {
                break
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    async fn test_one_cmd(cmd: &str, check_result: impl FnOnce(Vec<u8>)) {
        let (server, mut client, engine) = init_test().await;
        let (_, result) = client.command(cmd).await.unwrap();
        check_result(result);
        done_test(server, client, engine).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_key_one() {
        let cmd = "newkey";
        test_one_cmd(cmd, |result| assert_eq!(result.len(), 32)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_nonexist_account() {
        let account = "-1:5555555555555555555555555555555555555555555555555555555555555555";
        let cmd = format!(r#"getaccount {}"#, account);
        let mut etalon_result = String::from("{");
        etalon_result.push_str("\n\"");
        etalon_result.push_str("acc_type\":\t\"Nonexist");
        etalon_result.push_str("\"\n}");
        test_one_cmd(&cmd, |result| assert_eq!(result, etalon_result.as_bytes().to_vec())).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_active_account() {
        let account = "983217:0:000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F";
        let cmd = format!(r#"getaccount {}"#, account);
        let mut etalon_result = String::from("{");
        etalon_result.push_str("\n\"");
        etalon_result.push_str("acc_type\":\t\"Active");
        let etalon_result = etalon_result.as_bytes().to_vec();
        test_one_cmd(&cmd, |result| assert_eq!(result[..etalon_result.len()], etalon_result)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_account_state() {
        const OUT_FILE: &str = "./target/test_file.boc";
        let account = "983217:0:000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F";
        let cmd = format!(r#"getaccountstate {} {}"#, account, OUT_FILE);
        test_one_cmd(
            &cmd, 
            |result| { 
                assert_eq!(result.len(), 257);
                fs::remove_file(OUT_FILE).unwrap();
            }
        ).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stats() {

        fn check(result: Vec<u8>, ethalon: &[(&str, &str)]) {
            let stats = deserialize_boxed(&result).unwrap();
            let stats = downcast::<ton_api::ton::engine::validator::Stats>(stats).unwrap();
            // check if stats in JSON
            let json = format!("{:#}", stats_to_json(stats.stats().iter()));
            serde_json::from_str::<serde_json::Value>(&json).unwrap_or_else(
                |e| panic!("it must be JSON {e}")
            );
            for stat in stats.stats().iter() {
                match ethalon.iter().find(|(key, _)| key == &stat.key) {
                    Some((_, value)) => assert_eq!(&stat.value, value),
                    None => println!("unknown stats: {} => {}", stat.key, stat.value)
                }
            }
        }

        let ethalon_stats = [
            ("in_current_vset_p34", "false"),
            ("in_next_vset_p36", "false"),
            ("validation_stats", "{}"),
            ("collation_stats", "{}"),
            ("masterchainblocknumber", "0"),
            ("masterchainblocktime", "1"),
            ("sync_status","\"no_set_status\""),
            ("processed_workchain", "\"not specified\""),
            ("public_overlay_key_id", "\"b52Bb8xv6roWwIDGQnBIdL7hsl3Sy3Ro9ITEV9UlIz8=\""),
            ("tps_10", "0"),
            ("tps_300", "0"),
            ("validation_status", "\"Active\""),
        ];
        test_one_cmd("getstats", |result| check(result, &ethalon_stats)).await;

        let ethalon_stats_new = [
            ("in_current_vset_p34", "false"),
            ("in_next_vset_p36", "false"),
            ("global_id", "0"),
            ("last_validation_ago_sec", "{}"),
            ("last_collation_ago_sec", "{}"),
            ("masterchainblocknumber", "0"),
            ("masterchainblocktime", "1"),
            ("node_status","\"no_set_status\""),
            ("processed_workchain", "\"not specified\""),
            ("public_overlay_key_id", "\"b52Bb8xv6roWwIDGQnBIdL7hsl3Sy3Ro9ITEV9UlIz8=\""),
            ("tps_10", "0"),
            ("tps_300", "0"),
            ("validation_status", "\"Active\""),
        ];
        test_one_cmd("getstatsnew", |result| check(result, &ethalon_stats_new)).await;

    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_election_bid() {
        const OUT_FILE: &str = "./target/test_file.boc";
        let now = now() + 86400;
        let cmd = format!("election-bid {} {} \"{}\"", now, now + 10001, OUT_FILE);
        test_one_cmd(
            &cmd, 
            |result| {
                assert_eq!(result.len(), 164);
                fs::remove_file(OUT_FILE).unwrap();
            }
        ).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_stake() {
        const OUT_FILE: &str = "./target/test_file.boc";
        let cmd = format!("recover_stake \"{}\"", OUT_FILE);
        test_one_cmd(
            &cmd, 
            |result| {
                assert_eq!(result.len(), 25);
                fs::remove_file(OUT_FILE).unwrap();
            }
        ).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_config_param() {
        const IN_FILE: &str = "./target/zerostate.json";
        const OUT_FILE: &str = "./target/test_file.boc";
        std::fs::write(IN_FILE, SAMPLE_ZERO_STATE).unwrap();
        let cmd = format!("cparam 23 \"{}\" \"{}\"", IN_FILE, OUT_FILE);
        test_one_cmd(
            &cmd, 
            |result| {
                assert_eq!(result.len(), 53);
                let limits = BlockLimits::construct_from_bytes(&result).unwrap();
                assert_eq!(
                    limits.bytes(), 
                    &ParamLimits::with_limits(131072, 524288, 1048576).unwrap()
                );
                assert_eq!(
                    limits.gas(), 
                    &ParamLimits::with_limits(900000, 1200000, 2000000).unwrap()
                );
                assert_eq!(
                    limits.lt_delta(), 
                    &ParamLimits::with_limits(1000, 5000, 10000).unwrap()
                );
                fs::remove_file(IN_FILE).unwrap();
                fs::remove_file(OUT_FILE).unwrap();
            }
        ).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_key_with_export() {
        let (server, mut client, engine) = init_test().await;
        let (_, result) = client.command("newkey").await.unwrap();
        assert_eq!(result.len(), 32);
        let cmd = format!("exportpub {}", base64_encode(&result));
        let (_, result) = client.command(&cmd).await.unwrap();
        assert_eq!(result.len(), 32);
        done_test(server, client, engine).await;
    }

    macro_rules! parse_test {
        ($func:expr, $param:expr) => {
            $func($param.split_whitespace().next(), "test")
        };
    }

    #[test]
    fn test_parse_int() {
        assert_eq!(parse_test!(parse_int, "0").unwrap(), 0);
        assert_eq!(parse_test!(parse_int, "-1").unwrap(), -1);
        assert_eq!(parse_test!(parse_int, "1600000000").unwrap(), 1600000000);
        parse_test!(parse_int, "qwe").expect_err("must generate error");
        parse_int(Option::<&str>::None, "test").expect_err("must generate error");
    }

    #[test]
    fn test_parse_int256() {
        let ethalon = "GfgI79Xf3q7r4q1SPz7wAqBt0W6CjavuADODoz/DQE8=";
        assert_eq!(
            parse_test!(parse_int256, ethalon).unwrap(), 
            ethalon.parse::<UInt256>().unwrap()
        );
        assert_eq!(
            parse_test!(
                parse_int256, 
                "19F808EFD5DFDEAEEBE2AD523F3EF002A06DD16E828DABEE003383A33FC3404F"
            ).unwrap(), 
            ethalon.parse::<UInt256>().unwrap()
        );
        parse_test!(parse_int256, "11").expect_err("must generate error");
        parse_int256(Option::<&str>::None, "test").expect_err("must generate error");
    }

    #[test]
    fn test_parse_data() {
        let ethalon = vec![10, 77];
        assert_eq!(parse_test!(parse_data, "0A4D").unwrap(), ethalon);
        parse_test!(parse_data, "QQ").expect_err("must generate error");
        parse_test!(parse_data, "GfgI79Xf3q7r4q1SPz7wAqBt0W6CjavuADODoz/DQE8=")
            .expect_err("must generate error");
        parse_data(Option::<&str>::None, "test").expect_err("must generate error");
    }

    #[test]
    fn test_stats_to_json() {
        let mut stats = [OneStat { key: "key".to_string(), value: String::new()}];
        let value = stats_to_json(stats.iter());
        let ethalon = json!({"key": "null"});
        assert_eq!(ethalon, value);

        stats[0].value = "12345".into();
        let value = stats_to_json(stats.iter());
        let ethalon = json!({"key": 12345});
        assert_eq!(ethalon, value);

        stats[0].value = "\"Some text\"".into();
        let value = stats_to_json(stats.iter());
        let ethalon = json!({"key": "Some text"});
        assert_eq!(ethalon, value);

        stats[0].value = "\"777\"".into();
        let value = stats_to_json(stats.iter());
        let ethalon = json!({"key": "777"});
        assert_eq!(ethalon, value);

        stats[0].value = json!({"a": 777}).to_string();
        let value = stats_to_json(stats.iter());
        let ethalon = json!({"key": {"a": 777}});
        assert_eq!(ethalon, value);
    }

}
