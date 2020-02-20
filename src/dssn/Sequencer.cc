/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "Sequencer.h"

namespace DSSN {

Sequencer::Sequencer() : weight(0), last_usec(0) {}

u_int64_t Sequencer::readPHC()    // return PHC time stamp
{
    return 0; // for now
}

} // DSSN

