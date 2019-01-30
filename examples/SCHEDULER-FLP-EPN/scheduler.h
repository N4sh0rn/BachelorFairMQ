
/**
 * scheduler.h
 *
 *
 */

#ifndef FAIRMQEXAMPLESCHEDULERFLPEPNSCHEDULER_H
#define FAIRMQEXAMPLESCHEDULERFLPEPNSCHEDULER_H

#include <chrono>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include "FairMQDevice.h"
#include "Message.h"

namespace example_SCHEDULER_FLP_EPN {

class scheduler : public FairMQDevice {
 public:
  scheduler();
  virtual ~scheduler();
  void sender(std::vector<uint64_t>* vec, uint64_t* num, uint64_t* numE, uint64_t* schedNum);

 protected:
  // for EPN SCHEDULER DEALING
  struct EpnInfo {
    uint64_t ts = 0;
    uint64_t seqId = 0;
    uint64_t memVal = 0;
  };
  std::map<uint64_t, std::vector<EpnInfo> > history;
  std::map<uint64_t, bool> epnOnline;
  unsigned onlineEPNs = 0;


  uint64_t numEPNS;
  uint64_t numFLPS;
  const unsigned intMs;
  uint64_t programTime;
  const unsigned historyMaxMs;

  // constants to generate array sent to FLPs
  int epnsInScheduleRR;      // the amount of subtimeframes the FLPs send to the EPNs per second
  uint64_t amountEPNs;       // amount of EPNs that are in the array the Scheduler needs to send to the FLPs
  const float intervalFLPs;  // the interval in seconds in which the scheduler needs to send array to all FLPs

  // for SCHEDULER FLP DEALING
  std::vector<uint64_t> vectorForFlps;
  std::stringstream availableEpns1;
  std::stringstream EpnsInSchedule1;
  std::stringstream heatdata1;

  // needed for history processing
  uint64_t tooOld;                 // key which is tooOld (timestamp)
  uint64_t keyForToFile;           // reference key for writing history to File
  uint64_t keyForGeneratingArray;  // reference key for generating the array
  uint64_t keyForExiting;
  uint64_t scheduleNumber;
  int m;

  std::thread senderThread(std::vector<uint64_t>* vec, uint64_t* num, uint64_t* numE, uint64_t* schedNum);

  virtual bool ConditionalRun() override;
  virtual void InitTask() override;
  uint64_t getHistKey();
  void initialize(uint64_t numEPNs);
  void printHist();
  void update(const EPNtoScheduler &);
  void toFile();
  // what I want to return in this function is a pointer to the first element of the array
  std::vector<uint64_t> generateSchedule();
  std::vector<int> generateArray1();
  int maxSearch(std::vector<int>& arr);  // find the EPN with the most free memory, returns the index of the EPN with
                                         // most free memory
  void printVecFLP(std::vector<uint64_t> a);
  void printfreeSlots(int arr[], int length);
  int availableEpns(const std::vector<int>& arr, uint64_t siz) const;
  int EpnsInSchedule(int avaiable, uint64_t siz);
  std::vector<uint64_t> simpleRRSched(int m);  // function to print the array that will be sent to all FLPs
};
}

#endif /* FAIRMQEXAMPLESCHEDULERFLPEPNSCHEDULER_H */
