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

#ifndef RAMCLOUD_NOTIFIER_H
#define RAMCLOUD_NOTIFIER_H

#include "RpcWrapper.h"

namespace RAMCloud {

class Notifier {
  public:
    static void notify(Context* context, const WireFormat::Opcode type,
		       const void* message, const uint32_t length,
		       ServerId& id);
    static void notify(Context* context, const WireFormat::Opcode type,
		       const void* message, const uint32_t length,
		       Transport::SessionRef* endpoint = NULL,
		       const char* serviceLocator = NULL);
  private:
    Notifier();
};

class NotificationRpc : public RpcWrapper {
  public:
    NotificationRpc(Context* context, const char* serviceLocator,
		    const WireFormat::Opcode type, const void* message,
		    const uint32_t length);
    NotificationRpc(Context* context, const Transport::SessionRef session,
		    const WireFormat::Opcode type, const void* message,
		    const uint32_t length);
    /// \copydoc RpcWrapper::docForWait
    void wait();

  PRIVATE:
    Context* context;
    DISALLOW_COPY_AND_ASSIGN(NotificationRpc);
};
}
#endif /* RAMCLOUD_NOTIFIER_H */
