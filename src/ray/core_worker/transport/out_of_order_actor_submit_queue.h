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

#pragma once

#include <map>
#include <utility>

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/core_worker/transport/actor_submit_queue.h"

namespace ray {
namespace core {

/**
 * OutofOrderActorSubmitQueue sends request as soon as the dependencies are resolved.
 */
class OutofOrderActorSubmitQueue : public IActorSubmitQueue {
 public:
  OutofOrderActorSubmitQueue(ActorID actor_id);
  bool Emplace(uint64_t position, TaskSpecification spec) override;
  bool Contains(uint64_t position) const override;
  const std::pair<TaskSpecification, bool> &Get(uint64_t position) const override;
  void MarkDependencyFailed(uint64_t position) override;
  void MarkDependencyResolved(uint64_t position) override;
  std::vector<TaskID> ClearAllTasks() override;
  absl::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() override;
  std::map<uint64_t, TaskSpecification> PopAllOutOfOrderCompletedTasks() override;
  void OnClientConnected() override;
  uint64_t GetSequenceNumber(const TaskSpecification &task_spec) const override;
  void MarkTaskCompleted(uint64_t position, TaskSpecification task_spec) override;

 private:
  ActorID actor_id;
  std::map<uint64_t, std::pair<TaskSpecification, bool>> requests;
};
}  // namespace core
}  // namespace ray