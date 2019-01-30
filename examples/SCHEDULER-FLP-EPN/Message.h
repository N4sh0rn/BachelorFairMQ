/* Message.h */

#ifndef MESSAGE_H
#define MESSAGE_H

#include <string>
#include <vector>

namespace example_SCHEDULER_FLP_EPN {

struct EPNtoScheduler {
  int Id;
  uint64_t seqId;
  uint64_t freeSlots;
  uint64_t numEPNs;
};

struct SchedulerToFLP {
  int IdForFirstEpn;
  int IdForSecondEpn;
  int IdForThirdEpn;
};
struct FLPtoEPN {
  int IdOfFlp;
  unsigned long sTF;
  int schedNum;
};

struct SchedFLPTest {
  int IdForFirst;
};
}

#endif
