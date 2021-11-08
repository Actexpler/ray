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

#include "ray/core_worker/transport/out_of_order_actor_submit_queue.h"

namespace ray {
namespace core {
OutofOrderActorSubmitQueue::OutofOrderActorSubmitQueue(ActorID actor_id)
    : actor_id(actor_id) {}

bool OutofOrderActorSubmitQueue::Emplace(uint64_t position, TaskSpecification spec) {
  return requests.emplace(position, std::make_pair(spec, /*dependency_resolved*/ false))
      .second;
}

bool OutofOrderActorSubmitQueue::Contains(uint64_t position) const {
  return requests.find(position) != requests.end();
}

const std::pair<TaskSpecification, bool> &OutofOrderActorSubmitQueue::Get(
    uint64_t position) const {
  auto it = requests.find(position);
  RAY_CHECK(it != requests.end());
  return it->second;
}

void OutofOrderActorSubmitQueue::MarkDependencyFailed(uint64_t position) {
  requests.erase(position);
}

void OutofOrderActorSubmitQueue::MarkDependencyResolved(uint64_t position) {
  auto it = requests.find(position);
  RAY_CHECK(it != requests.end());
  it->second.second = true;
}

std::vector<TaskID> OutofOrderActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (auto &[pos, spec] : requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  requests.clear();
  return task_ids;
}

absl::optional<std::pair<TaskSpecification, bool>>
OutofOrderActorSubmitQueue::PopNextTaskToSend() {
  for (auto it = requests.begin(); it != requests.end(); it++) {
    if (/*dependencies_resolved*/ it->second.second) {
      auto task_spec = std::move(it->second.first);
      requests.erase(it);
      return std::make_pair(std::move(task_spec), /*skip_queue*/ true);
    }
  }
  return absl::nullopt;
}

std::map<uint64_t, TaskSpecification>
OutofOrderActorSubmitQueue::PopAllOutOfOrderCompletedTasks() {
  return {};
}

void OutofOrderActorSubmitQueue::OnClientConnected() {}

uint64_t OutofOrderActorSubmitQueue::GetSequenceNumber(
    const TaskSpecification &task_spec) const {
  return task_spec.ActorCounter();
}

void OutofOrderActorSubmitQueue::MarkTaskCompleted(uint64_t position,
                                                   TaskSpecification task_spec) {}

}  // namespace core
}  // namespace ray