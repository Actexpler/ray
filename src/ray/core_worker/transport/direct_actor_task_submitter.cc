// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/transport/direct_actor_task_submitter.h"

#include <thread>

#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {
namespace core {

void CoreWorkerDirectActorTaskSubmitter::AddActorQueueIfNotExists(
    const ActorID &actor_id) {
  absl::MutexLock lock(&mu_);
  // No need to check whether the insert was successful, since it is possible
  // for this worker to have multiple references to the same actor.
  client_queues_.emplace(actor_id, ClientQueue(actor_id));
}

void CoreWorkerDirectActorTaskSubmitter::KillActor(const ActorID &actor_id,
                                                   bool force_kill, bool no_restart) {
  absl::MutexLock lock(&mu_);
  rpc::KillActorRequest request;
  request.set_intended_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);

  auto it = client_queues_.find(actor_id);
  // The language frontend can only kill actors that it has a reference to.
  RAY_CHECK(it != client_queues_.end());

  if (!it->second.pending_force_kill) {
    it->second.pending_force_kill = request;
  } else if (force_kill) {
    // Overwrite the previous request to kill the actor if the new request is a
    // force kill.
    it->second.pending_force_kill->set_force_kill(true);
    if (no_restart) {
      // Overwrite the previous request to disable restart if the new request's
      // no_restart flag is set to true.
      it->second.pending_force_kill->set_no_restart(true);
    }
  }

  SendPendingTasks(actor_id);
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  auto task_id = task_spec.TaskId();
  auto actor_id = task_spec.ActorId();
  RAY_LOG(DEBUG) << "Submitting task " << task_id;
  RAY_CHECK(task_spec.IsActorTask());

  bool task_queued = false;
  uint64_t send_pos = 0;
  {
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (queue->second.state != rpc::ActorTableData::DEAD) {
      // We must fix the send order prior to resolving dependencies, which may
      // complete out of order. This ensures that we will not deadlock due to
      // backpressure. The receiving actor will execute the tasks according to
      // this sequence number.
      send_pos = task_spec.ActorCounter();
      RAY_CHECK(queue->second.actor_submit_queue->Emplace(send_pos, task_spec));
      task_queued = true;
    }
  }

  if (task_queued) {
    // We must release the lock before resolving the task dependencies since
    // the callback may get called in the same call stack.
    resolver_.ResolveDependencies(task_spec, [this, send_pos, actor_id](Status status) {
      absl::MutexLock lock(&mu_);
      auto queue = client_queues_.find(actor_id);
      RAY_CHECK(queue != client_queues_.end());
      auto &actor_submit_queue = queue->second.actor_submit_queue;
      // Only dispatch tasks if the submitted task is still queued. The task
      // may have been dequeued if the actor has since failed.
      if (actor_submit_queue->Contains(send_pos)) {
        if (status.ok()) {
          actor_submit_queue->MarkDependencyResolved(send_pos);
          SendPendingTasks(actor_id);
        } else {
          auto task_id = actor_submit_queue->Get(send_pos).first.TaskId();
          actor_submit_queue->MarkDependencyFailed(send_pos);
          task_finisher_.PendingTaskFailed(
              task_id, rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status);
        }
      }
    });
  } else {
    // Do not hold the lock while calling into task_finisher_.
    task_finisher_.MarkTaskCanceled(task_id);
    std::shared_ptr<rpc::RayException> creation_task_exception = nullptr;
    {
      absl::MutexLock lock(&mu_);
      auto queue = client_queues_.find(task_spec.ActorId());
      creation_task_exception = queue->second.creation_task_exception;
    }
    auto status = Status::IOError("cancelling task of dead actor");
    // No need to increment the number of completed tasks since the actor is
    // dead.
    RAY_UNUSED(!task_finisher_.PendingTaskFailed(task_id, rpc::ErrorType::ACTOR_DIED,
                                                 &status, creation_task_exception));
  }

  // If the task submission subsequently fails, then the client will receive
  // the error in a callback.
  return Status::OK();
}

void CoreWorkerDirectActorTaskSubmitter::DisconnectRpcClient(ClientQueue &queue) {
  queue.rpc_client = nullptr;
  core_worker_client_pool_.Disconnect(WorkerID::FromBinary(queue.worker_id));
  queue.worker_id.clear();
  queue.pending_force_kill.reset();
}

void CoreWorkerDirectActorTaskSubmitter::FailInflightTasks(
    const std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
        &inflight_task_callbacks) {
  // NOTE(kfstorm): We invoke the callbacks with a bad status to act like there's a
  // network issue. We don't call `task_finisher_.PendingTaskFailed` directly because
  // there's much more work to do in the callback.
  auto status = Status::IOError("Fail all inflight tasks due to actor state change.");
  rpc::PushTaskReply reply;
  for (const auto &entry : inflight_task_callbacks) {
    entry.second(status, reply);
  }
}

void CoreWorkerDirectActorTaskSubmitter::ConnectActor(const ActorID &actor_id,
                                                      const rpc::Address &address,
                                                      int64_t num_restarts) {
  RAY_LOG(DEBUG) << "Connecting to actor " << actor_id << " at worker "
                 << WorkerID::FromBinary(address.worker_id());

  std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
      inflight_task_callbacks;

  {
    absl::MutexLock lock(&mu_);

    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (num_restarts < queue->second.num_restarts) {
      // This message is about an old version of the actor and the actor has
      // already restarted since then. Skip the connection.
      RAY_LOG(INFO) << "Skip actor connection that has already been restarted, actor_id="
                    << actor_id;
      return;
    }

    if (queue->second.rpc_client &&
        queue->second.rpc_client->Addr().ip_address() == address.ip_address() &&
        queue->second.rpc_client->Addr().port() == address.port()) {
      RAY_LOG(DEBUG) << "Skip actor that has already been connected, actor_id="
                     << actor_id;
      return;
    }

    if (queue->second.state == rpc::ActorTableData::DEAD) {
      // This message is about an old version of the actor and the actor has
      // already died since then. Skip the connection.
      return;
    }

    queue->second.num_restarts = num_restarts;
    if (queue->second.rpc_client) {
      // Clear the client to the old version of the actor.
      DisconnectRpcClient(queue->second);
      inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks);
      queue->second.inflight_task_callbacks.clear();
    }

    queue->second.state = rpc::ActorTableData::ALIVE;
    // Update the mapping so new RPCs go out with the right intended worker id.
    queue->second.worker_id = address.worker_id();
    // Create a new connection to the actor.
    queue->second.rpc_client = core_worker_client_pool_.GetOrConnect(address);
    queue->second.actor_submit_queue->OnClientConnected();

    RAY_LOG(INFO) << "Connecting to actor " << actor_id << " at worker "
                  << WorkerID::FromBinary(address.worker_id());
    ResendOutOfOrderTasks(actor_id);
    SendPendingTasks(actor_id);
  }

  // NOTE(kfstorm): We need to make sure the lock is released before invoking callbacks.
  FailInflightTasks(inflight_task_callbacks);
}

void CoreWorkerDirectActorTaskSubmitter::DisconnectActor(
    const ActorID &actor_id, int64_t num_restarts, bool dead,
    const std::shared_ptr<rpc::RayException> &creation_task_exception) {
  RAY_LOG(DEBUG) << "Disconnecting from actor " << actor_id;

  std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
      inflight_task_callbacks;

  {
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (!dead) {
      RAY_CHECK(num_restarts > 0);
    }
    if (num_restarts <= queue->second.num_restarts && !dead) {
      // This message is about an old version of the actor that has already been
      // restarted successfully. Skip the message handling.
      RAY_LOG(INFO)
          << "Skip actor disconnection that has already been restarted, actor_id="
          << actor_id;
      return;
    }

    // The actor failed, so erase the client for now. Either the actor is
    // permanently dead or the new client will be inserted once the actor is
    // restarted.
    DisconnectRpcClient(queue->second);
    inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks);
    queue->second.inflight_task_callbacks.clear();

    if (dead) {
      queue->second.state = rpc::ActorTableData::DEAD;
      queue->second.creation_task_exception = creation_task_exception;
      // If there are pending requests, treat the pending tasks as failed.
      RAY_LOG(INFO) << "Failing pending tasks for actor " << actor_id
                    << " because the actor is already dead.";

      auto status = Status::IOError("cancelling all pending tasks of dead actor");
      auto task_ids = queue->second.actor_submit_queue->ClearAllTasks();

      for (auto &task_id : task_ids) {
        task_finisher_.MarkTaskCanceled(task_id);
        // No need to increment the number of completed tasks since the actor is
        // dead.
        RAY_UNUSED(!task_finisher_.PendingTaskFailed(task_id, rpc::ErrorType::ACTOR_DIED,
                                                     &status, creation_task_exception));
      }

      auto &wait_for_death_info_tasks = queue->second.wait_for_death_info_tasks;

      RAY_LOG(INFO) << "Failing tasks waiting for death info, size="
                    << wait_for_death_info_tasks.size() << ", actor_id=" << actor_id;
      for (auto &net_err_task : wait_for_death_info_tasks) {
        RAY_UNUSED(task_finisher_.MarkPendingTaskFailed(
            net_err_task.second, rpc::ErrorType::ACTOR_DIED, creation_task_exception));
      }

      // No need to clean up tasks that have been sent and are waiting for
      // replies. They will be treated as failed once the connection dies.
      // We retain the sequencing information so that we can properly fail
      // any tasks submitted after the actor death.
    } else if (queue->second.state != rpc::ActorTableData::DEAD) {
      // Only update the actor's state if it is not permanently dead. The actor
      // will eventually get restarted or marked as permanently dead.
      queue->second.state = rpc::ActorTableData::RESTARTING;
      queue->second.num_restarts = num_restarts;
    }
  }

  // NOTE(kfstorm): We need to make sure the lock is released before invoking callbacks.
  FailInflightTasks(inflight_task_callbacks);
}

void CoreWorkerDirectActorTaskSubmitter::CheckTimeoutTasks() {
  absl::MutexLock lock(&mu_);
  for (auto &queue_pair : client_queues_) {
    auto &queue = queue_pair.second;
    auto deque_itr = queue.wait_for_death_info_tasks.begin();
    while (deque_itr != queue.wait_for_death_info_tasks.end() &&
           /*timeout timestamp*/ deque_itr->first < current_time_ms()) {
      auto task_spec = deque_itr->second;
      task_finisher_.MarkPendingTaskFailed(task_spec, rpc::ErrorType::ACTOR_DIED);
      deque_itr = queue.wait_for_death_info_tasks.erase(deque_itr);
    }
  }
}

void CoreWorkerDirectActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  if (!it->second.rpc_client) {
    return;
  }
  auto &client_queue = it->second;

  // Check if there is a pending force kill. If there is, send it and disconnect the
  // client.
  if (client_queue.pending_force_kill) {
    RAY_LOG(INFO) << "Sending KillActor request to actor " << actor_id;
    // It's okay if this fails because this means the worker is already dead.
    client_queue.rpc_client->KillActor(*client_queue.pending_force_kill, nullptr);
    client_queue.pending_force_kill.reset();
  }

  // Submit all pending actor_submit_queue->
  auto &actor_submit_queue = client_queue.actor_submit_queue;

  while (true) {
    auto task = actor_submit_queue->PopNextTaskToSend();
    if (!task.has_value()) {
      break;
    }
    RAY_CHECK(!client_queue.worker_id.empty());
    PushActorTask(client_queue, task.value().first, task.value().second);
  }
}

void CoreWorkerDirectActorTaskSubmitter::ResendOutOfOrderTasks(const ActorID &actor_id) {
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  if (!it->second.rpc_client) {
    return;
  }
  auto &client_queue = it->second;
  RAY_CHECK(!client_queue.worker_id.empty());
  auto out_of_order_completed_tasks =
      client_queue.actor_submit_queue->PopAllOutOfOrderCompletedTasks();
  for (const auto &completed_task : out_of_order_completed_tasks) {
    // Making a copy here because we are flipping a flag and the original value is
    // const.
    auto task_spec = completed_task.second;
    task_spec.GetMutableMessage().set_skip_execution(true);
    PushActorTask(client_queue, task_spec, /*skip_queue=*/true);
  }
}

void CoreWorkerDirectActorTaskSubmitter::PushActorTask(ClientQueue &queue,
                                                       const TaskSpecification &task_spec,
                                                       bool skip_queue) {
  auto request = std::make_unique<rpc::PushTaskRequest>();
  // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());

  request->set_intended_worker_id(queue.worker_id);
  request->set_sequence_number(queue.actor_submit_queue->GetSequenceNumber(task_spec));

  const auto task_id = task_spec.TaskId();
  const auto actor_id = task_spec.ActorId();
  const auto actor_counter = task_spec.ActorCounter();
  const auto task_skipped = task_spec.GetMessage().skip_execution();
  const auto num_queued =
      request->sequence_number() - queue.rpc_client->ClientProcessedUpToSeqno();
  RAY_LOG(DEBUG) << "Pushing task " << task_id << " to actor " << actor_id
                 << " actor counter " << actor_counter << " seq no "
                 << request->sequence_number() << " num queued " << num_queued;
  if (num_queued >= next_queueing_warn_threshold_) {
    // TODO(ekl) add more debug info about the actor name, etc.
    warn_excess_queueing_(actor_id, num_queued);
    next_queueing_warn_threshold_ *= 2;
  }

  rpc::Address addr(queue.rpc_client->Addr());
  rpc::ClientCallback<rpc::PushTaskReply> reply_callback =
      [this, addr, task_id, actor_id, actor_counter, task_spec, task_skipped](
          const Status &status, const rpc::PushTaskReply &reply) {
        bool increment_completed_tasks = true;

        if (task_skipped) {
          // NOTE(simon):Increment the task counter regardless of the status because the
          // reply for a previously completed task. We are not calling CompletePendingTask
          // because the tasks are pushed directly to the actor, not placed on any queues
          // in task_finisher_.
        } else if (status.ok()) {
          task_finisher_.CompletePendingTask(task_id, reply, addr);
        } else {
          // push task failed due to network error. For example, actor is dead
          // and no process response for the push task.
          absl::MutexLock lock(&mu_);
          auto queue_pair = client_queues_.find(actor_id);
          RAY_CHECK(queue_pair != client_queues_.end());
          auto &queue = queue_pair->second;

          bool immediately_mark_object_fail = (queue.state == rpc::ActorTableData::DEAD);
          bool will_retry = task_finisher_.PendingTaskFailed(
              task_id, rpc::ErrorType::ACTOR_DIED, &status, queue.creation_task_exception,
              immediately_mark_object_fail);
          if (will_retry) {
            increment_completed_tasks = false;
          } else if (!immediately_mark_object_fail) {
            // put it to wait_for_death_info_tasks and wait for Death info
            int64_t death_info_timeout_ts =
                current_time_ms() +
                RayConfig::instance().timeout_ms_task_wait_for_death_info();
            queue.wait_for_death_info_tasks.emplace_back(death_info_timeout_ts,
                                                         task_spec);
            RAY_LOG(INFO)
                << "PushActorTask failed because of network error, this task "
                   "will be stashed away and waiting for Death info from GCS, task_id="
                << task_spec.TaskId()
                << ", wait queue size=" << queue.wait_for_death_info_tasks.size();
          }
        }

        if (increment_completed_tasks) {
          absl::MutexLock lock(&mu_);
          auto queue_pair = client_queues_.find(actor_id);
          RAY_CHECK(queue_pair != client_queues_.end());
          auto &queue = queue_pair->second;
          queue.actor_submit_queue->MarkTaskCompleted(actor_counter, task_spec);
        }
      };

  queue.inflight_task_callbacks.emplace(task_id, std::move(reply_callback));
  rpc::ClientCallback<rpc::PushTaskReply> wrapped_callback =
      [this, task_id, actor_id](const Status &status, const rpc::PushTaskReply &reply) {
        rpc::ClientCallback<rpc::PushTaskReply> reply_callback;
        {
          absl::MutexLock lock(&mu_);
          auto it = client_queues_.find(actor_id);
          RAY_CHECK(it != client_queues_.end());
          auto &queue = it->second;
          auto callback_it = queue.inflight_task_callbacks.find(task_id);
          if (callback_it == queue.inflight_task_callbacks.end()) {
            RAY_LOG(DEBUG) << "The task " << task_id
                           << " has already been marked as failed. Ingore the reply.";
            return;
          }
          reply_callback = std::move(callback_it->second);
          queue.inflight_task_callbacks.erase(callback_it);
        }
        reply_callback(status, reply);
      };

  queue.rpc_client->PushActorTask(std::move(request), skip_queue, wrapped_callback);
}

bool CoreWorkerDirectActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);

  auto iter = client_queues_.find(actor_id);
  return (iter != client_queues_.end() && iter->second.rpc_client);
}
}  // namespace core
}  // namespace ray
