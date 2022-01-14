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

pub use super::*;

/// Task of task queue
pub type TaskPtr = Box<dyn FnOnce(&mut dyn SessionProcessor) + Send>;

/// Pointer to the task queue
pub type TaskQueuePtr = Arc<dyn TaskQueue<TaskPtr>>;

/// Session callback task
pub type CallbackTaskPtr = Box<dyn FnOnce() + Send>;

/// Pointer to session callback task queue
pub type CallbackTaskQueuePtr = Arc<dyn TaskQueue<CallbackTaskPtr>>;

/// Identifier of completion handler for multi-threaded to single-threaded callbacks forwarding
pub type CompletionHandlerId = u64;

/// Completion handler
pub type CompletionHandlerPtr = Box<dyn CompletionHandler>;

/// Task queue
pub trait TaskQueue<FuncPtr: Send + 'static>: Send + Sync {
    /// Is queue overloaded
    fn is_overloaded(&self) -> bool;

    /// Is queue empty
    fn is_empty(&self) -> bool;

    /// Post closure (non-generic interface)
    fn post_closure(&self, task: FuncPtr);

    /// Pull closure
    fn pull_closure(
        &self,
        timeout: std::time::Duration,
        last_warn_dump_time: &mut std::time::SystemTime,
    ) -> Option<FuncPtr>;
}

/// Post closure to be run in a main processing thread
pub(crate) fn post_closure<F>(queue: &TaskQueuePtr, task_fn: F)
where
    F: FnOnce(&mut dyn SessionProcessor),
    F: Send + 'static,
{
    queue.post_closure(Box::new(task_fn));
}

/// Post closure to be run in a session callbacks processing thread
pub(crate) fn post_callback_closure<F>(queue: &CallbackTaskQueuePtr, task_fn: F)
where
    F: FnOnce(),
    F: Send + 'static,
{
    queue.post_closure(Box::new(task_fn));
}

/// Completion handler processor
pub trait CompletionHandlerProcessor {
    /// Task queue for completion handlers
    fn get_completion_task_queue(&self) -> &TaskQueuePtr;

    /// Add completion handler
    fn add_completion_handler(&mut self, handler: CompletionHandlerPtr) -> CompletionHandlerId;

    /// Remove completion handler
    fn remove_completion_handler(
        &mut self,
        handler_id: CompletionHandlerId,
    ) -> Option<CompletionHandlerPtr>;
}

/// Create completion handler
pub(crate) fn create_completion_handler<T, F>(
    completion_handler_processor: &mut dyn CompletionHandlerProcessor,
    response_callback: F,
) -> Box<dyn FnOnce(Result<T>) + Send>
where
    T: 'static + Send,
    F: FnOnce(Result<T>, &mut dyn SessionProcessor) + 'static,
{
    let response_callback = Box::new(response_callback);
    let handler_index = completion_handler_processor.add_completion_handler(Box::new(
        SingleThreadedCompletionHandler::<T>::new(response_callback),
    ));
    let queue_weak_ptr = Arc::downgrade(
        &completion_handler_processor
            .get_completion_task_queue()
            .clone(),
    );

    let handler = move |result: Result<T>| {
        if let Some(mut queue) = queue_weak_ptr.upgrade() {
            post_closure(&mut queue, move |processor: &mut dyn SessionProcessor| {
                if let Some(mut handler) = processor.remove_completion_handler(handler_index) {
                    if let Some(handler) = handler
                        .get_mut_impl()
                        .downcast_mut::<SingleThreadedCompletionHandler<T>>()
                    {
                        if let Some(handler) = handler.handler.take() {
                            handler(result, processor);
                        }
                    } else {
                        unreachable!();
                    }
                }
            })
        }
    };

    Box::new(handler)
}

/// Completion handler interface
pub trait CompletionHandler {
    ///Time of handler creation
    fn get_creation_time(&self) -> std::time::SystemTime;

    ///Execute with error
    fn reset_with_error(&mut self, error: failure::Error, receiver: &mut dyn SessionProcessor);

    /// Cast to Any
    fn get_mut_impl(&mut self) -> &mut dyn std::any::Any;
}

/// Completion handler wrapper for single-threaded usage
struct SingleThreadedCompletionHandler<T> {
    handler: Option<Box<dyn FnOnce(Result<T>, &mut dyn SessionProcessor)>>,
    creation_time: std::time::SystemTime,
}

impl<T> SingleThreadedCompletionHandler<T> {
    fn new(handler: Box<dyn FnOnce(Result<T>, &mut dyn SessionProcessor)>) -> Self {
        Self {
            handler: Some(handler),
            creation_time: std::time::SystemTime::now(),
        }
    }
}

impl<T> CompletionHandler for SingleThreadedCompletionHandler<T>
where
    T: 'static,
{
    ///Time of handler creation
    fn get_creation_time(&self) -> std::time::SystemTime {
        self.creation_time
    }

    ///Execute handler with error
    fn reset_with_error(&mut self, error: failure::Error, receiver: &mut dyn SessionProcessor) {
        if let Some(handler) = self.handler.take() {
            handler(Err(error), receiver);
        }
    }

    /// Cast to Any
    fn get_mut_impl(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
