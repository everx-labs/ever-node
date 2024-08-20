
/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

use super::workchain::Workchain;
use super::workchain::WorkchainPtr;
use super::utils::HangCheck;
use super::*;
use log::*;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::Duration;
use std::time::UNIX_EPOCH;
use spin::mutex::SpinMutex;
use ton_api::ton::ton_node::broadcast::BlockCandidateBroadcast;
use ever_block::Result;
use catchain::utils::MetricsDumper;
use catchain::utils::MetricsHandle;
use catchain::PublicKeyHash;
use metrics::Recorder;
use catchain::utils::compute_instance_counter;
use catchain::check_execution_time;
use catchain::utils::get_elapsed_time;

/*
    Constants
*/

const METRICS_DUMP_PERIOD_MS: u64 = 60000; //time for verification manager metrics dump

/*
===============================================================================
    VerificationManagerImplImpl
===============================================================================
*/

/// Hash map for workchains
type WorkchainMap = HashMap<i32, WorkchainPtr>;

/// Pointer to workchains map
type WorkchainMapPtr = Arc<WorkchainMap>;

/// Verficator manager
pub struct VerificationManagerImpl {
    engine: EnginePtr,                                                 //pointer to engine
    runtime: tokio::runtime::Handle,                                   //runtime for spawns
    workchains: Arc<SpinMutex<WorkchainMapPtr>>,                       //map of workchains
    should_stop_flag: Arc<AtomicBool>,                                 //flag to indicate manager should be stopped
    dump_thread_is_stopped_flag: Arc<AtomicBool>,                      //flag to indicate dump thread is stopped
    _metrics_receiver: MetricsHandle,                                  //metrics receiver
    blocks_instance_counter: Arc<InstanceCounter>,                     //instance counter for blocks
    workchains_instance_counter: Arc<InstanceCounter>,                 //instance counter for workchains
    wc_overlays_instance_counter: Arc<InstanceCounter>,                //instance counter for workchains WC overlays
    mc_overlays_instance_counter: Arc<InstanceCounter>,                //instance counter for workchains MC overlays
    send_new_block_candidate_counter: metrics::Counter,  //counter for new candidates invocations
    update_workchains_counter: metrics::Counter,         //counter for workchains update invocations
    config: VerificationManagerConfig,                                 //verification manager config
}

#[async_trait::async_trait]
impl VerificationManager for VerificationManagerImpl {
    /*
        Block candidates management
    */

    /// New block broadcast has been generated
    async fn send_new_block_candidate(&self, candidate: &BlockCandidate) {
        check_execution_time!(20_000);

        log::debug!(target:"verificator", "New block candidate has been generated {:?}", candidate.block_id);

        let _hang_checker = HangCheck::new(self.runtime.clone(), format!("VerificationManagerImpl::send_new_block_candidate: {:?}", candidate.block_id), Duration::from_millis(1000));

        let workchain_id = candidate.block_id.shard_id.workchain_id();
        let workchain = match self.workchains.lock().get(&workchain_id) {
            Some(workchain) => Some(workchain.clone()),
            None => None,
        };

        if let Some(workchain) = workchain {
            let block_id = candidate.block_id.clone();
            let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n.as_millis(),
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            };
            let candidate = BlockCandidateBroadcast {
                id: candidate.block_id.clone(),
                data: candidate.data.clone().into(),
                collated_data: candidate.collated_data.clone().into(),
                collated_data_file_hash: candidate.collated_file_hash.clone().into(),
                created_by: candidate.created_by.clone(),
                created_timestamp: timestamp as i64,
            };
            let workchain = workchain.clone();

            self.send_new_block_candidate_counter.increment(1);

            if let Err(err) = self.runtime
                .spawn(async move {
                    workchain.send_new_block_candidate(candidate);
                })
                .await
            {
                error!(target: "verificator", "Error during new candidate {:?} send: {:?}", block_id, err);
            }
        }
    }

    /// Get block status (delivered, rejected)
    fn get_block_status(
        &self,
        block_id: &BlockIdExt,
    ) -> (bool, bool) {
        let workchain_id = block_id.shard_id.workchain_id();
        let workchain = match self.workchains.lock().get(&workchain_id) {
            Some(workchain) => Some(workchain.clone()),
            None => None,
        };

        if let Some(workchain) = workchain {
            if let Some(block) = workchain.get_block_by_id(&block_id) {
                return workchain.get_block_status(&block);
            }
        }

        (false, false)
    }

    /// Wait for block verification
    fn wait_for_block_verification(
        &self,
        block_id: &BlockIdExt,
        timeout: Option<std::time::Duration>,
    ) -> bool {
        log::trace!(target: "verificator", "Start block {} verification", block_id);

        let workchain_id = block_id.shard_id.workchain_id();
        let workchain = match self.workchains.lock().get(&workchain_id) {
            Some(workchain) => Some(workchain.clone()),
            None => None,
        };

        if let Some(workchain) = workchain {
            let smft_config = workchain.get_config();
            let start_time = SystemTime::now();
            let timeout = if let Some(timeout) = timeout {
                timeout
            } else {
                if let Some(max_mc_delivery_wait_timeout) = self.config.max_mc_delivery_wait_timeout {
                    //node's config may override SMFT network config
                    max_mc_delivery_wait_timeout
                } else {
                    //use SMFT network config
                    Duration::from_millis(smft_config.mc_max_delivery_waiting_timeout_ms as u64)
                }
            };

            loop {
                  //check block status

                if let Some(block) = workchain.get_block_by_id(&block_id) {
                    let (delivered, rejected) = workchain.get_block_status(&block);

                    workchain.update_block_external_delivery_metrics(&block_id, &start_time);

                    if rejected {
                        log::warn!(target: "verificator", "Finish block {} verification - NACK detected", block_id);

                        workchain.start_arbitrage(&block_id);

                        return false;
                    }

                    if delivered && !rejected {
                        log::trace!(target: "verificator", "Finish block {} verification - DELIVERED", block_id);
                        return true;
                    }
                }

                  //check for timeout

                let elapsed_time = get_elapsed_time(&start_time);
                if elapsed_time > timeout {
                    log::warn!(target: "verificator", "Can't verify block {}: timeout {}ms expired. Start force block delivery", block_id, elapsed_time.as_millis());
                    workchain.update_block_external_delivery_metrics(&block_id, &start_time);

                    workchain.start_force_block_delivery(&block_id);

                    if timeout.as_millis() == 0 {
                        //special case: in case of mc_max_delivery_wait_timeout = 0 - work in shadow mode and allow unverified blocks to be included in MC
                        log::warn!(target: "verificator", "Can't verify block {}: block is included in MC due to SMFT shadow mode (mc_max_delivery_wait_timeout=0)", block_id);
                        return true;
                    }

                    return false;
                }

                  //sleep

                const WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);
                std::thread::sleep(WAIT_TIMEOUT);
            }
        }

        log::warn!(target: "verificator", "Can't verify block {}: no workchain found", block_id);

        false
    }    

    /*
        Workchains management
    */

    /*
    /// Reset workchains
    async fn reset_workchains<'a>(
        &'a self,
    ) {
        check_execution_time!(100_000);

        let _hang_checker = HangCheck::new(self.runtime.clone(), "VerificationManagerImpl::reset_workchains".to_string(), Duration::from_millis(1000));

        trace!(target: "verificator", "Reset workchains");

        self.update_workchains_counter.increment(1);

        self.set_workchains(Arc::new(HashMap::new()));
    }
    */    

    /// Update workchains
    async fn update_workchains<'a>(
        &'a self,
        local_key_id: PublicKeyHash,
        local_bls_key: PrivateKey,
        workchain_id: i32,
        utime_since: u32,
        workchain_validators: &'a Vec<ValidatorDescr>,
        mc_validators: &'a Vec<ValidatorDescr>,
        listener: &'a VerificationListenerPtr,
        use_debug_bls_keys: bool,
    ) {
        check_execution_time!(100_000);

        let _hang_checker = HangCheck::new(self.runtime.clone(), "VerificationManagerImpl::update_workchains".to_string(), Duration::from_millis(1000));

        trace!(target: "verificator", "Update workchains");

        self.update_workchains_counter.increment(1);

        //create workchains

        let engine = self.engine.clone();
        let current_workchains = self.get_workchains();
        let mut new_workchains = HashMap::new();

        match Self::get_workchain_by_validator_set(
            &engine,
            self.runtime.clone(),
            &local_key_id,
            &local_bls_key,
            &current_workchains,
            workchain_id,
            utime_since,
            &workchain_validators,
            mc_validators,
            listener,
            self.workchains_instance_counter.clone(),
            self.blocks_instance_counter.clone(),
            self.wc_overlays_instance_counter.clone(),
            self.mc_overlays_instance_counter.clone(),
            use_debug_bls_keys,
        )
        .await
        {
            Ok(workchain) => {
                new_workchains.insert(workchain_id, workchain);
            }
            Err(err) => {
                error!(target: "verificator", "Can't create workchain: {:?}", err);
            }
        };

        //replace workchains cache with a new one

        self.set_workchains(Arc::new(new_workchains));
    }
}

impl VerificationManagerImpl {
    /*
        Workchains management
    */

    /// Get workchains map
    fn get_workchains(&self) -> WorkchainMapPtr {
        self.workchains.lock().clone()
    }

    /// Set workchains map
    fn set_workchains(&self, workchains: WorkchainMapPtr) {
        *self.workchains.lock() = workchains;
    }

    /// Compute validator set hash based on a validators list
    fn compute_validator_set_hash(utime_since: u32, validators: &Vec<ValidatorDescr>) -> UInt256 {
        let mut result = Vec::<u8>::with_capacity(validators.len() * 32);

        for validator in validators {
            result.extend(validator.public_key.key_bytes());
        }

        result.extend(utime_since.to_le_bytes());

        UInt256::calc_file_hash(&result)
    }

    async fn get_workchain_by_validator_set(
        engine: &EnginePtr,
        runtime: tokio::runtime::Handle,
        local_key_id: &PublicKeyHash,
        local_bls_key: &PrivateKey,
        workchains: &WorkchainMapPtr,
        workchain_id: i32,
        utime_since: u32,
        wc_validators: &Vec<ValidatorDescr>,
        mc_validators: &Vec<ValidatorDescr>,
        listener: &VerificationListenerPtr,
        workchains_instance_counter: Arc<InstanceCounter>,
        blocks_instance_counter: Arc<InstanceCounter>,
        wc_overlays_instance_counter: Arc<InstanceCounter>,
        mc_overlays_instance_counter: Arc<InstanceCounter>,
        use_debug_bls_keys: bool,
    ) -> Result<WorkchainPtr> {
        let wc_validator_set_hash = Self::compute_validator_set_hash(utime_since, wc_validators);
        let mc_validator_set_hash = Self::compute_validator_set_hash(utime_since, mc_validators);

        //try to find workchain in a cache based on its ID and hash

        if let Some(workchain) = workchains.get(&workchain_id) {
            if workchain.get_wc_validator_set_hash() == &wc_validator_set_hash && 
               workchain.get_mc_validator_set_hash() == &mc_validator_set_hash {
                return Ok(workchain.clone());
            }
        }

        //create new workchain

        let workchain = Workchain::create(
            engine.clone(),
            runtime,
            workchain_id,
            wc_validators.clone(),
            mc_validators.clone(),
            wc_validator_set_hash,
            mc_validator_set_hash,
            local_key_id,
            local_bls_key,
            listener.clone(),
            workchains_instance_counter,
            blocks_instance_counter,
            wc_overlays_instance_counter,
            mc_overlays_instance_counter,
            use_debug_bls_keys,
        )
        .await?;

        Ok(workchain)
    }

    /*
        Self-diagnostic
    */

    fn run_metrics_dumper(
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        metrics_receiver: MetricsHandle,
        workchains: Arc<SpinMutex<WorkchainMapPtr>>,
        ) {
        let builder = std::thread::Builder::new();

        let _ = builder.spawn(move || {
            let mut metrics_dumper = MetricsDumper::new();
            let mut workchain_metrics_dumpers: HashMap<i32, (MetricsDumper, i32)> = HashMap::new();

            metrics_dumper.add_compute_handler("smft_block".to_string(), &compute_instance_counter);
            metrics_dumper.add_compute_handler("smft_workchains".to_string(), &compute_instance_counter);
            metrics_dumper.add_compute_handler("smft_wc_overlays".to_string(), &compute_instance_counter);
            metrics_dumper.add_compute_handler("smft_mc_overlays".to_string(), &compute_instance_counter);

            metrics_dumper.add_derivative_metric("smft_block".to_string());
            metrics_dumper.add_derivative_metric("smft_workchains".to_string());

            let mut next_metrics_dump_time = SystemTime::now() + Duration::from_millis(METRICS_DUMP_PERIOD_MS);
            let mut loop_idx = 0;

            loop {
                if should_stop_flag.load(Ordering::SeqCst) {
                    break;
                }

                const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

                std::thread::sleep(CHECKING_INTERVAL);

                if let Err(_err) = next_metrics_dump_time.elapsed() {
                    continue;
                }

                if !log_enabled!(target: "verificator", log::Level::Debug) {
                    continue;
                }

                metrics_dumper.update(&metrics_receiver);

                debug!(target: "verificator", "Verification manager workchains dump:");

                let current_workchains = workchains.lock().clone();

                for (workchain_id, workchain) in current_workchains.iter() {
                    let mut builder = string_builder::Builder::default();

                    workchain.dump_state(&mut builder);

                    if let Ok(result) = builder.string() {
                        debug!(target: "verificator", "{}", result);
                    } else {
                        debug!(target: "verificator", "Error during workchain #{} dump", workchain_id);
                    }
                }

                debug!(target: "verificator", "Verification manager metrics:");

                metrics_dumper.dump(|string| {
                    debug!(target: "verificator", "{}", string);
                });

                #[cfg(not(feature = "statsd"))]
                metrics_dumper.enumerate_as_f64(|key, value| {
                    metrics::gauge!(key.replace(".", "_"), value);
                });

                for (workchain_id, workchain) in current_workchains.iter() {
                    let mut dumper = workchain_metrics_dumpers.get_mut(workchain_id);

                    if dumper.is_none() {
                        let mut metrics_dumper = MetricsDumper::new();

                        workchain.configure_dumper(&mut metrics_dumper);

                        workchain_metrics_dumpers.insert(*workchain_id, (metrics_dumper, loop_idx));
                        dumper = workchain_metrics_dumpers.get_mut(workchain_id);
                    }

                    let dumper = dumper.unwrap();

                    let (workchain_metrics_dumper, updated_loop_idx) = dumper;

                    *updated_loop_idx = loop_idx;

                    workchain_metrics_dumper.update(&workchain.get_metrics_receiver());

                    workchain_metrics_dumper.dump(|string| {
                        debug!(target: "verificator", "{}", string);
                    });
                }

                workchain_metrics_dumpers.retain(|_, dumper| dumper.1 == loop_idx);

                next_metrics_dump_time = SystemTime::now() + Duration::from_millis(METRICS_DUMP_PERIOD_MS);
                loop_idx += 1;
            }

            debug!(target: "verificator", "Verification manager dump loop is finished");

            is_stopped_flag.store(true, Ordering::SeqCst);
        }).unwrap();
    }

    /*
        Stopping
    */

    pub fn stop(&self) {
        log::info!(target: "verificator", "Stopping verification manager");

        self.should_stop_flag.store(true, Ordering::SeqCst);

        loop {
            if self.dump_thread_is_stopped_flag.load(Ordering::SeqCst) {
                break;
            }

            const MAX_WAIT : std::time::Duration = std::time::Duration::from_millis(500);

            std::thread::sleep(MAX_WAIT);

            debug!(target: "verificator", "...waiting for verification manager stopping");
        }

        log::info!(target: "verificator", "Verification manager has been stopped");
    }

    /*
        Constructor
    */

    pub fn create(engine: EnginePtr, runtime: tokio::runtime::Handle, config: VerificationManagerConfig) -> Arc<dyn VerificationManager> {
        log::info!(target: "verificator", "Creating verification manager");

        let workchains = Arc::new(SpinMutex::new(Arc::new(HashMap::new())));
        let metrics_receiver = MetricsHandle::new(Some(Duration::from_secs(30)));
        let should_stop_flag = Arc::new(AtomicBool::new(false));
        let dump_thread_is_stopped_flag = Arc::new(AtomicBool::new(false));

        let body = Self {
            engine,
            runtime,
            workchains: workchains.clone(),
            _metrics_receiver: metrics_receiver.clone(),
            send_new_block_candidate_counter: metrics_receiver.sink().register_counter(&"smft_candidates".into()),
            update_workchains_counter: metrics_receiver.sink().register_counter(&"smft_workchains_updates".into()),
            blocks_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"smft_block".to_string())),
            workchains_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"smft_workchains".to_string())),
            wc_overlays_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"smft_wc_overlays".to_string())),
            mc_overlays_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"smft_mc_overlays".to_string())),
            should_stop_flag: should_stop_flag.clone(),
            dump_thread_is_stopped_flag: dump_thread_is_stopped_flag.clone(),
            config,
        };

        Self::run_metrics_dumper(should_stop_flag, dump_thread_is_stopped_flag, metrics_receiver, workchains);

        Arc::new(body)
    }
}

impl Drop for VerificationManagerImpl {
    fn drop(&mut self) {
        self.stop();
    }
}
