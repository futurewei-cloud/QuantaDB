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

/* Copyright (c) 2010-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.lie
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "Status.h"

namespace RAMCloud {

class StatusTest : public ::testing::Test {
  public:
    StatusTest() { }

  private:
    DISALLOW_COPY_AND_ASSIGN(StatusTest);
};

TEST_F(StatusTest, statusToString) {
    // Make sure that Status value 0 always corresponds to success.
    EXPECT_STREQ("operation succeeded",
            statusToString(Status(0)));
    EXPECT_STREQ("object has wrong version",
            statusToString(STATUS_WRONG_VERSION));
    EXPECT_TRUE(statusToString(Status(STATUS_MAX_VALUE)) !=
                    statusToString(Status(STATUS_MAX_VALUE + 1)));
    EXPECT_STREQ("unrecognized Status (35)",
            statusToString(Status(STATUS_MAX_VALUE+1)));
}

TEST_F(StatusTest, statusToSymbol) {
    // Make sure that Status value 0 always corresponds to success.
    EXPECT_STREQ("STATUS_OK",
            statusToSymbol(Status(0)));
    EXPECT_STREQ("STATUS_WRONG_VERSION",
            statusToSymbol(STATUS_WRONG_VERSION));
    EXPECT_TRUE(statusToSymbol(Status(STATUS_MAX_VALUE)) !=
                    statusToSymbol(Status(STATUS_MAX_VALUE + 1)));
    EXPECT_STREQ("STATUS_UNKNOWN",
            statusToSymbol(Status(STATUS_MAX_VALUE+1)));
}

}  // namespace RAMCloud
