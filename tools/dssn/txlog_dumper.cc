/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TxLog.h"
#include "Cycles.h"

using namespace DSSN;

TxLog *txlog;

int main(int ac, char *av[])
{
    txlog = new TxLog(true);

    printf("log size: %ld\n", txlog->size());
    txlog->dump(1);
    return 0;
}
