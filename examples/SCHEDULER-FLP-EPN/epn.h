/**
 * epn.h
 *
 *
 *
 */

#ifndef FAIRMQEXAMPLESCHEDULERFLPEPNEPN_H
#define FAIRMQEXAMPLESCHEDULERFLPEPNEPN_H

#include <time.h>
#include <atomic>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include "FairMQDevice.h"
#include "Message.h"

namespace example_SCHEDULER_FLP_EPN {

class epn : public FairMQDevice {
 public:
  epn();
  virtual ~epn();


 protected:
  uint64_t Id;
  std::atomic_int freeSlots;
  uint64_t seqId = 0;
  uint64_t maxSlots;
  uint64_t numEPNS;
  uint64_t numFLPS;
  std::map<unsigned long, int> rcvdSTFs;
  std::map<unsigned long, int>::iterator it;
  int sTF;

  const float procTime;
  const float procDev;

  uint64_t startTime;
  uint64_t timeBetweenTf;
  uint64_t programTime;  // the duration of the program.
  const unsigned intMs;

  std::stringstream receptionOfTf1;
  std::stringstream processingTime1;
  std::stringstream numberOfLostTfs1;

  virtual void InitTask();
  virtual bool ConditionalRun();

  std::string getChannel(char number);
  bool receive();
  void send();

  void MyDelayedFun(float delayWork);
  float getDelay();

  uint64_t getHistKey();
};
}

#endif /* FAIRMQEXAMPLESCHEDULERFLPEPNEPN_H */
