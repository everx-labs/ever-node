
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
use ton_types::Result;
use catchain::utils::MetricsDumper;
use catchain::utils::compute_instance_counter;
use catchain::profiling::check_execution_time;

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
    metrics_receiver: Arc<metrics_runtime::Receiver>,                  //metrics receiver
    blocks_instance_counter: Arc<InstanceCounter>,                     //instance counter for blocks
    workchains_instance_counter: Arc<InstanceCounter>,                 //instance counter for workchains
    wc_overlays_instance_counter: Arc<InstanceCounter>,                //instance counter for workchains WC overlays
    mc_overlays_instance_counter: Arc<InstanceCounter>,                //instance counter for workchains MC overlays
    send_new_block_candidate_counter: metrics_runtime::data::Counter,  //counter for new candidates invocations
    update_workchains_counter: metrics_runtime::data::Counter,         //counter for workchains update invocations
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

            self.send_new_block_candidate_counter.increment();

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
        collated_data_file_hash: &UInt256,
        created_by: &UInt256,
    ) -> (bool, bool) {
        let workchain_id = block_id.shard_id.workchain_id();
        let workchain = match self.workchains.lock().get(&workchain_id) {
            Some(workchain) => Some(workchain.clone()),
            None => None,
        };

        if let Some(workchain) = workchain {
            let candidate_id =
                Workchain::get_candidate_id_impl(block_id, collated_data_file_hash, created_by);

            if let Some(block) = workchain.get_block_by_id(&candidate_id) {
                return workchain.get_block_status(&block);
            }
        }

        (false, false)
    }

    /*
        Workchains management
    */

    /// Update workchains
    async fn update_workchains<'a>(
        &'a self,
        local_key: PrivateKey,
        local_bls_key: PrivateKey,
        workchain_id: i32,
        workchain_validators: &'a Vec<ValidatorDescr>,
        mc_validators: &'a Vec<ValidatorDescr>,
        listener: &'a VerificationListenerPtr,
    ) {
        check_execution_time!(100_000);

        let _hang_checker = HangCheck::new(self.runtime.clone(), "VerificationManagerImpl::update_workchains".to_string(), Duration::from_millis(1000));

        trace!(target: "verificator", "Update workchains");

        self.update_workchains_counter.increment();

        //create workchains

        let engine = self.engine.clone();
        let current_workchains = self.get_workchains();
        let mut new_workchains = HashMap::new();

        match Self::get_workchain_by_validator_set(
            &engine,
            self.runtime.clone(),
            &local_key,
            &local_bls_key,
            &current_workchains,
            workchain_id,
            &workchain_validators,
            mc_validators,
            listener,
            self.metrics_receiver.clone(),
            self.workchains_instance_counter.clone(),
            self.blocks_instance_counter.clone(),
            self.wc_overlays_instance_counter.clone(),
            self.mc_overlays_instance_counter.clone(),
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
    fn compute_validator_set_hash(validators: &Vec<ValidatorDescr>) -> UInt256 {
        let mut result = Vec::<u8>::with_capacity(validators.len() * 32);

        for validator in validators {
            result.extend(validator.public_key.key_bytes());
        }

        UInt256::calc_file_hash(&result)
    }

    async fn get_workchain_by_validator_set(
        engine: &EnginePtr,
        runtime: tokio::runtime::Handle,
        local_key: &PrivateKey,
        local_bls_key: &PrivateKey,
        workchains: &WorkchainMapPtr,
        workchain_id: i32,
        validators: &Vec<ValidatorDescr>,
        mc_validators: &Vec<ValidatorDescr>,
        listener: &VerificationListenerPtr,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
        workchains_instance_counter: Arc<InstanceCounter>,
        blocks_instance_counter: Arc<InstanceCounter>,
        wc_overlays_instance_counter: Arc<InstanceCounter>,
        mc_overlays_instance_counter: Arc<InstanceCounter>,
    ) -> Result<WorkchainPtr> {
        let validator_set_hash = Self::compute_validator_set_hash(validators);

        //try to find workchain in a cache based on its ID and hash

        if let Some(workchain) = workchains.get(&workchain_id) {
            if workchain.get_validator_set_hash() == &validator_set_hash {
                return Ok(workchain.clone());
            }
        }

        //create new workchain

        let workchain = Workchain::create(
            engine.clone(),
            runtime,
            workchain_id,
            validators.clone(),
            mc_validators.clone(),
            validator_set_hash,
            local_key,
            local_bls_key,
            listener.clone(),
            metrics_receiver,
            workchains_instance_counter,
            blocks_instance_counter,
            wc_overlays_instance_counter,
            mc_overlays_instance_counter,
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
        metrics_receiver: Arc<metrics_runtime::Receiver>,
        workchains: Arc<SpinMutex<WorkchainMapPtr>>,
        ) {
        let builder = std::thread::Builder::new();

        let _ = builder.spawn(move || {
            let mut metrics_dumper = MetricsDumper::new();
            let mut workchain_metrics_dumpers: HashMap<i32, (MetricsDumper, i32)> = HashMap::new();

            metrics_dumper.add_compute_handler("verificator_block".to_string(), &compute_instance_counter);
            metrics_dumper.add_compute_handler("verificator_workchains".to_string(), &compute_instance_counter);
            metrics_dumper.add_compute_handler("verificator_wc_overlays".to_string(), &compute_instance_counter);
            metrics_dumper.add_compute_handler("verificator_mc_overlays".to_string(), &compute_instance_counter);

            metrics_dumper.add_derivative_metric("verificator_block".to_string());
            metrics_dumper.add_derivative_metric("verificator_workchains".to_string());

            let mut next_metrics_dump_time = SystemTime::now();
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

                debug!(target: "verificator", "Verification manager metrics:");

                metrics_dumper.dump(|string| {
                    debug!(target: "verificator", "{}", string);
                });

                let current_workchains = workchains.lock().clone();

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

                    workchain_metrics_dumper.update(&metrics_receiver);

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

    pub fn create(engine: EnginePtr, runtime: tokio::runtime::Handle) -> Arc<dyn VerificationManager> {
        log::info!(target: "verificator", "Creating verification manager");

        let workchains = Arc::new(SpinMutex::new(Arc::new(HashMap::new())));
        let metrics_receiver = Arc::new(metrics_runtime::Receiver::builder().build().expect("failed to create metrics receiver"));
        let should_stop_flag = Arc::new(AtomicBool::new(false));
        let dump_thread_is_stopped_flag = Arc::new(AtomicBool::new(false));

        let body = Self {
            engine,
            runtime,
            workchains: workchains.clone(),
            metrics_receiver: metrics_receiver.clone(),
            send_new_block_candidate_counter: metrics_receiver.sink().counter("verificator_candidates"),
            update_workchains_counter: metrics_receiver.sink().counter("verificator_workchains_updates"),
            blocks_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"verificator_block".to_string())),
            workchains_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"verificator_workchains".to_string())),
            wc_overlays_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"verificator_wc_overlays".to_string())),
            mc_overlays_instance_counter: Arc::new(InstanceCounter::new(&metrics_receiver, &"verificator_mc_overlays".to_string())),
            should_stop_flag: should_stop_flag.clone(),
            dump_thread_is_stopped_flag: dump_thread_is_stopped_flag.clone(),
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
