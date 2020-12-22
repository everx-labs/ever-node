pub use super::*;

use std::any::Any;

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
pub type CompletionHandler = Box<dyn Any>;

/// Task queue
pub trait TaskQueue<FuncPtr: Send + 'static>: Send + Sync {
    /// Post closure (non-generic interface)
    fn post_closure(&self, task: FuncPtr);

    /// Pull closure
    fn pull_closure(&self, timeout: std::time::Duration) -> Option<FuncPtr>;
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
    /// Task queue
    fn get_task_queue(&self) -> &TaskQueuePtr;

    /// Add completion handler
    fn add_completion_handler(&mut self, handler: CompletionHandler) -> CompletionHandlerId;

    /// Remove completion handler
    fn remove_completion_handler(
        &mut self,
        handler_id: CompletionHandlerId,
    ) -> Option<CompletionHandler>;
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
        SingleThreadedCompletionHandler::new(response_callback),
    ));
    let queue_weak_ptr = Arc::downgrade(&completion_handler_processor.get_task_queue().clone());

    let handler = move |result: Result<T>| {
        if let Some(mut queue) = queue_weak_ptr.upgrade() {
            post_closure(&mut queue, move |processor: &mut dyn SessionProcessor| {
                if let Some(mut handler) = processor.remove_completion_handler(handler_index) {
                    if let Some(handler) =
                        handler.downcast_mut::<SingleThreadedCompletionHandler<T>>()
                    {
                        let handler = handler.handler.take();
                        (handler.unwrap())(result, processor);
                    }
                }
            })
        }
    };

    Box::new(handler)
}

/// Completion handler wrapper for single-threaded usage
struct SingleThreadedCompletionHandler<T> {
    handler: Option<Box<dyn FnOnce(Result<T>, &mut dyn SessionProcessor)>>,
}

impl<T> SingleThreadedCompletionHandler<T> {
    fn new(handler: Box<dyn FnOnce(Result<T>, &mut dyn SessionProcessor)>) -> Self {
        Self {
            handler: Some(handler),
        }
    }
}
