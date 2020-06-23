/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include <algorithm>
#include "TxLog.h"

using namespace DSSN;

TxLog *txlog;

void Usage(char *prog)
{
    printf("Usage %s [-dump|-clear]\n", prog);
    exit (1);
}

int main(int ac, char *av[])
{
    txlog = new TxLog(true);

    if (ac == 1)
        Usage(av[0]);

    for (uint32_t idx = 1; idx < ac; idx++) {
        if (strcmp(av[idx], "-dump") == 0) {
            if (txlog->size() == 0) {
                printf("TxLog is empty\n");
            } else
                txlog->dump(1);
        } else 
        if (strcmp(av[idx], "-clear") == 0) {
            txlog->clear();
        } else {
            printf("Unknown option: %s\n", av[idx]);
            Usage(av[0]);
        }
    }
    return 0;
}
