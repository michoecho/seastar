#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2026 ScyllaDB
#

#
# Fail if EXECUTABLE, together with the shared libraries it depends on, has
# any symbol which cannot be resolved.  `ld --no-undefined` does not cover
# this: it constrains the objects being linked, not the shared libraries they
# are linked against, whose symbols are resolved only at load time.
#
# Run as a script: cmake -DEXECUTABLE=... -DLDD=... -P this-file
#

execute_process (
  COMMAND ${LDD} -r ${EXECUTABLE}
  OUTPUT_VARIABLE output
  ERROR_VARIABLE output
  RESULT_VARIABLE result)

if (NOT result EQUAL 0)
  message (FATAL_ERROR "Failed to inspect ${EXECUTABLE}:\n${output}")
endif ()

string (REGEX MATCHALL "undefined symbol:[^\n]*" undefined "${output}")

if (undefined)
  string (REPLACE ";" "\n  " undefined "${undefined}")
  message (FATAL_ERROR
    "${EXECUTABLE} has unresolved symbols:\n  ${undefined}\n"
    "The simulator is missing a definition which the RPC layer needs.")
endif ()
