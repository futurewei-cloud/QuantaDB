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
#include <sys/types.h>
#include <dirent.h>
#include "TxLog.h"

using namespace QDB;

TxLog *txlog;

void Usage(char *prog)
{
    printf("Usage %s -list   # list existing txlogs \n", prog);
    printf("Usage %s logname [-dump|-clear]\n", prog);
    exit (1);
}

int main(int ac, char *av[])
{
    if (ac < 2)
        Usage(av[0]);

    DIR *dir;
    // List
    if (strcmp(av[1], "-list") == 0) {
        if ((dir = opendir(TXLOG_DIR)) == NULL) {
            printf("%s: txlog not exist\n", TXLOG_DIR);
            exit (2);
        }
        struct dirent *dp;
        while ((dp = readdir(dir)) != NULL) {
            if ((strcmp(dp->d_name, ".") == 0) ||
                (strcmp(dp->d_name, "..") == 0) ||
                (dp->d_type != DT_DIR))
                continue;
            printf("%s\n", dp->d_name);
        }
        exit (0);
    }

    if (ac < 3)
        Usage(av[0]);

    // sanity check
    char *logname = av[1];
    char txlog_dir[128];
    sprintf(txlog_dir, "%s/%s", TXLOG_DIR, logname); 

    if ((dir = opendir(txlog_dir)) == NULL) {
        printf("%s: txlog not exist\n", txlog_dir);
        exit (2);
    }
    closedir(dir);

    // instantiate txlog
    txlog = new TxLog(true, logname);

    for (uint32_t idx = 2; idx < ac; idx++) {
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
