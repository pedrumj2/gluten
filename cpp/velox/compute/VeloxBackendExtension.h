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

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "velox/common/config/Config.h"

namespace gluten {

/// A callback run during VeloxBackend::init().
///
/// The intended use is registering Velox entities that a build links directly into
/// its own binary -- scalar functions, aggregate functions, file systems, reader and
/// writer factories, data sinks, connectors -- in a distribution of Gluten that
/// carries code Gluten itself does not know about.
///
/// The existing route for native functions is the config key
/// spark.gluten.sql.columnar.backend.velox.udfLibraryPaths, which dlopens a shared
/// library at runtime. That is the right answer for a function shipped independently of
/// the binary, and this changes nothing about it. It has nothing to offer a function
/// that is already linked in, though: there is no library to load. Such a build
/// currently has to patch VeloxBackend::init() itself, which conflicts on every rebase.
///
/// The callback receives the resolved backend config so an extension can honour the
/// session's settings instead of reading global state.
///
/// A callback runs BEFORE Velox's global memory manager is initialized, so it must not
/// allocate and must not call facebook::velox::memory::memoryManager(). That throws
/// "The memory manager is not set", and a callback that instead reaches
/// deprecatedDefaultMemoryManager() creates the singleton with default options, after
/// which VeloxBackend::init() fails outright on "The memory manager has already been
/// set". Register factories here and let them build their objects lazily.
using VeloxBackendExtension = std::function<void(const facebook::velox::config::ConfigBase&)>;

/// Registers `extension` under `name`, which is used only in log and error messages.
///
/// Must be called before VeloxBackend::create(); an extension registered after the
/// backend has initialized is never run, and registering one then throws rather than
/// leaving the caller to wonder why nothing happened. Throws if `name` is already
/// registered.
///
/// Registering from a static initializer works, but only if the translation unit
/// holding it is actually linked in. An object file whose sole contribution is a
/// static initializer is dropped from a static archive, because nothing references a
/// symbol in it. Either call this from code that is already on a live path, or link
/// the extension with --whole-archive.
void registerVeloxBackendExtension(const std::string& name, VeloxBackendExtension extension);

/// The names registered so far, in registration order. Exposed for tests and for
/// diagnostics.
std::vector<std::string> registeredVeloxBackendExtensions();

/// Runs every registered extension, in registration order, then clears the registry
/// and marks it closed. Called by VeloxBackend::init(); not intended to be called
/// directly. Clearing is what makes a callback run at most once even if
/// VeloxBackend::create() is called again.
void initVeloxBackendExtensions(const facebook::velox::config::ConfigBase& conf);

/// Drops every registered extension and reopens the registry. For tests only; the
/// `testing` prefix follows Velox's convention for such helpers.
void testingUnregisterAllVeloxBackendExtensions();

} // namespace gluten
