/* Copyright 2020 Futurewei Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#pragma once

#include <string.h>

using namespace std;

struct hash_c_str {
	size_t operator()(const char *str)
	{
		string k(str);
		return hash<string>{}(k);
	}
};

struct equal_to_c_str {
	bool operator()(const char *k1, const char *k2)
	{
		return (k1 == k2) || (strcmp(k1, k2) == 0);
	}
};
