/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
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
