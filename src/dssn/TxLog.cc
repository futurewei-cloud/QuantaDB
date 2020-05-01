/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#include "TxLog.h"

namespace DSSN {

TxLog::TxLog()
{
    log = new DLog<TXLOG_CHUNK_SIZE>(TXLOG_DIR, false);
}

TxLog::TxLog(bool recovery_mode = false)
{
    log = new DLog<TXLOG_CHUNK_SIZE>(TXLOG_DIR, recovery_mode);
}

bool TxLog::add(TxEntry *txEntry)
{
    // Log CTS, validator, ReadSet, writeSet, 
    return true;
}

};
