<!---
# Copyright 2020 Futurewei Technologies, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
-->

# QuantaDB
# What is QuantaDB?
Coming Soon.


# Build Instruction
- **Checkout the Submodules**
git submodule update --init --recursive

- **Build Prometheus**
cd prometheus-cpp
mkdir build
cd build; cmake ..; make -j 20

- **Build QuantaDB**
make

