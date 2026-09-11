/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "compute/VeloxBackendExtension.h"

#include <mutex>
#include <utility>

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"

namespace gluten {
namespace {

struct Registry {
  std::mutex mutex;
  std::vector<std::pair<std::string, VeloxBackendExtension>> extensions;
  bool closed{false};
};

Registry& registry() {
  static auto* instance = new Registry();
  return *instance;
}

} // namespace

void registerVeloxBackendExtension(const std::string& name, VeloxBackendExtension extension) {
  VELOX_USER_CHECK_NOT_NULL(extension, "Velox backend extension '{}' is null.", name);

  auto& reg = registry();
  std::lock_guard<std::mutex> lock(reg.mutex);

  VELOX_USER_CHECK(
      !reg.closed,
      "Velox backend extension '{}' was registered after VeloxBackend::init() ran, so it would "
      "never execute. Register it before VeloxBackend::create().",
      name);

  for (const auto& registered : reg.extensions) {
    VELOX_USER_CHECK_NE(registered.first, name, "Velox backend extension '{}' is already registered.", name);
  }

  reg.extensions.emplace_back(name, std::move(extension));
}

std::vector<std::string> registeredVeloxBackendExtensions() {
  auto& reg = registry();
  std::lock_guard<std::mutex> lock(reg.mutex);

  std::vector<std::string> names;
  names.reserve(reg.extensions.size());
  for (const auto& registered : reg.extensions) {
    names.push_back(registered.first);
  }
  return names;
}

void initVeloxBackendExtensions(const facebook::velox::config::ConfigBase& conf) {
  auto& reg = registry();

  // The callbacks are moved out under the lock and run outside it: an extension is
  // free to register Velox entities that themselves reach back into this registry,
  // and holding the lock across a callback would deadlock. The explicit clear below
  // is what empties the registry -- a moved-from vector is guaranteed valid, not
  // guaranteed empty -- so a second VeloxBackend::create() does not run every
  // callback again.
  std::vector<std::pair<std::string, VeloxBackendExtension>> extensions;
  {
    std::lock_guard<std::mutex> lock(reg.mutex);
    extensions = std::move(reg.extensions);
    reg.extensions.clear();
    reg.closed = true;
  }

  for (const auto& [name, extension] : extensions) {
    LOG(INFO) << "Initializing Velox backend extension: " << name;
    extension(conf);
  }
}

void testingUnregisterAllVeloxBackendExtensions() {
  auto& reg = registry();
  std::lock_guard<std::mutex> lock(reg.mutex);
  reg.extensions.clear();
  reg.closed = false;
}

} // namespace gluten
