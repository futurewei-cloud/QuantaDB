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

/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include <algorithm>
#include "DataLog.h"

using namespace QDB;

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
