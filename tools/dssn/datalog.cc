/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include <algorithm>
#include "DataLog.h"

using namespace DSSN;

DataLog *log;

void Usage(char *prog)
{
    printf("Usage %s [-dump|-clear]\n", prog);
    exit (1);
}

int main(int ac, char *av[])
{
    log = new DataLog(999);

    if (ac == 1)
        Usage(av[0]);

    for (uint32_t idx = 1; idx < ac; idx++) {
        if (strcmp(av[idx], "-dump") == 0) {
            if (log->size() == 0) {
                printf("DataLog is empty\n");
            } else
                log->dump(1);
        } else 
        if (strcmp(av[idx], "-clear") == 0) {
            log->clear();
        } else {
            printf("Unknown option: %s\n", av[idx]);
            Usage(av[0]);
        }
    }
    return 0;
}
