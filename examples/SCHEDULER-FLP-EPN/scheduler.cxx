/*
 * scheduler.cpp
 *
 */

#include "scheduler.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>  // this_thread::sleep_for
#include <vector>
#include <mutex>
#include "Message.h"

using namespace std;

namespace example_SCHEDULER_FLP_EPN {

scheduler::scheduler()
    : history(),
      numEPNS(0),
      numFLPS(0),
      intMs(1000)  // one second
      ,
      programTime(0)  // alive time of the program
      ,
      historyMaxMs(59 * 1 * intMs)  // one minute
      ,
      epnsInScheduleRR(0),
      amountEPNs(0),
      intervalFLPs(1)  // interval of sending out schedule
      ,
      vectorForFlps(0),
      availableEpns1(),
      EpnsInSchedule1(),
      heatdata1(),
      tooOld(0),
      keyForToFile(0),
      keyForGeneratingArray(0),
      keyForExiting(0),
      scheduleNumber(0),
      m(1)

{}

void scheduler::InitTask() {
  numEPNS = fConfig->GetValue<uint64_t>("numEPNS");
  numFLPS = fConfig->GetValue<uint64_t>("numFLPS");
  amountEPNs = fConfig->GetValue<uint64_t>("amountEPNs");
  programTime = fConfig->GetValue<uint64_t>("programTime");
  initialize(numEPNS);
  std::this_thread::sleep_for(std::chrono::milliseconds(20000));
  keyForToFile = getHistKey();
  keyForGeneratingArray = getHistKey();
  keyForExiting = getHistKey();
}

bool scheduler::ConditionalRun() {

  // exit condition
  if ((getHistKey() - keyForExiting) > (programTime * 60 * 1000)) {
    // SEND MESSAGE TO FLPS SAYING IT's ALL 3
    vector<uint64_t> vect;
    for (int i = 0; i < amountEPNs; i++) {
      vect.push_back(3);
    }

    for (uint64_t i = 0; i < numFLPS; i++) {
      auto& mySendingChan = GetChannel("schedflp", i);
      FairMQMessagePtr message = mySendingChan.NewMessage((sizeof(uint64_t)) * vect.size());
      std::memcpy(message->GetData(), vect.data(), message->GetSize());
      mySendingChan.Send(message);
    }

    LOG(INFO) << "about to terminate the program!";
    ofstream ofHeatdata, ofEpnsInSchedule, ofAvailableEpns;
    ofHeatdata.open("heatdata.txt", std::ios_base::trunc);
    ofEpnsInSchedule.open("EpnsInSchedule.txt", std::ios_base::trunc);
    ofAvailableEpns.open("availableEpns.txt", std::ios_base::trunc);

    ofHeatdata << heatdata1.rdbuf();
    ofEpnsInSchedule << EpnsInSchedule1.rdbuf();
    ofAvailableEpns << availableEpns1.rdbuf();

    // flush and close because we exit before ofstream destructor is called
    ofHeatdata.flush();
    ofEpnsInSchedule.flush();
    ofAvailableEpns.flush();

    ofHeatdata.close();
    ofEpnsInSchedule.close();
    ofAvailableEpns.close();

    // make sure all messages are sent
    std::this_thread::sleep_for(std::chrono::seconds(long(10)));
    LOG(INFO) << "TERMINATING PROGRAM NOW!";
    exit(0);
    return false;
  }

  // receiving epn updates
  FairMQPollerPtr poller(NewPoller("epnsched"));
  poller->Poll(50);

  for (int i = 0; i < numEPNS; i++) {
    if (poller->CheckInput("epnsched", i)) {
      auto& myRecvChan = GetChannel("epnsched", i);
      // try to receive more updates
      // I expect this kind of messages
      EPNtoScheduler msgFromSender;

      // receive a message
      FairMQMessagePtr aMessage = myRecvChan.NewMessage();
      if (myRecvChan.Receive(aMessage) == sizeof(EPNtoScheduler)) {
        // get the data of the FairMQ message
        std::memcpy(&msgFromSender, aMessage->GetData(), sizeof(EPNtoScheduler));
        // LOG(INFO)<<"received ID: "<<msgFromSender.Id<<" and amount of free slots "<<msgFromSender.freeSlots<<"
        // and amount of EPNs is: "<< msgFromSender.numEPNs << endl;
        update(msgFromSender.Id, msgFromSender.freeSlots);
      } else {
        LOG(ERROR) << "Error while receiving EPN updates.";
        return false;
      }
    }
  }

  if (getHistKey() >= (keyForGeneratingArray + uint64_t(intervalFLPs * 1000.0))) {
    LOG(INFO) << "next time " << (keyForGeneratingArray + uint64_t(intervalFLPs * 1000.0));

    LOG(INFO) << "Creating the schedule " << scheduleNumber << " at time " << getHistKey() << " lastKey "
              << keyForGeneratingArray;
    LOG(INFO) << "intervalFLPs " << intervalFLPs;
    vectorForFlps = simpleRRSched(m);
    m = (m + amountEPNs) % numEPNS;
    int f = availableEpns(generateArray1(), numEPNS);
    LOG(INFO) << "Epns having at least one free memory slot: " << f;
    availableEpns1 << f << endl;
    // int inSchedule= EpnsInSchedule(f, amountEPNs);
    // LOG(INFO)<<"Epns in Schedule: "<< inSchedule;
    // EpnsInSchedule1<<inSchedule<<endl;
    EpnsInSchedule1 << epnsInScheduleRR << endl;
    epnsInScheduleRR = 0;
    // vectorForFlps = generateSchedule();
    // printVecFLP(vectorForFlps);
    keyForGeneratingArray = getHistKey();
    scheduleNumber++;
    std::thread t1 = senderThread(&vectorForFlps, &numFLPS, &amountEPNs, &scheduleNumber);
    t1.detach();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return true;
}

thread scheduler::senderThread(std::vector<uint64_t>* vec, uint64_t* num, uint64_t* numE, uint64_t* schedNum) {
  return std::thread([=] { this->sender(vec, num, numE, schedNum); });
}

uint64_t scheduler::getHistKey() {
  // get the current time
  const auto time = std::chrono::high_resolution_clock::now();
  // get the time from THEN to now
  auto duration = time.time_since_epoch();
  const std::uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  // std::cout << "Milliseconds : " << millis << std::endl;
  // determine the interval (key for the hist map)
  const std::uint64_t intKey = millis / 1000 * 1000;
  //     const std::uint64_t intKey = millis /  25   * 25;
  // cout<< "histKey : " << intKey << endl;

  return intKey;
}

void scheduler::initialize(uint64_t numberofEPNs) {
  // history is empty
  // get my key
  const auto key = getHistKey();

  if (history.count(key) == 0) {
    history.insert(pair<uint64_t, vector<EpnInfo>>(key, vector<EpnInfo>(numberofEPNs)));
    for (uint64_t a = 0; a < numEPNS; a++) {
      history[key].at(a).ts = key;
      history[key].at(a).memVal = 3;
    }
  }
}

void scheduler::printHist() {
  for (auto& keyVal : history) {
    // const auto Key = keyVal.first;
    const auto& Epns = keyVal.second;

    // std::cout << "TimeInterval: " << Key << endl;
    for (auto& epnI : Epns) {
      cout << epnI.ts << ":" << epnI.memVal << " ";
    }
  }
  cout << endl;
}

void scheduler::update(uint64_t epnId, uint64_t myMem) {
  // get the key (also current time)
  const auto key = getHistKey();

  // checking if the key already exists
  if (history.count(key) != 0) {
    history[key].at(epnId - 1).ts = key;
    history[key].at(epnId - 1).memVal = myMem;
  }
  // case that key does not exist.
  else {  // get the previous key
    auto prevKey = history.rbegin();
    uint64_t prevKeyInt = prevKey->first;

    // insert a new vector
    history.insert(pair<uint64_t, vector<EpnInfo>>(key, vector<EpnInfo>(numEPNS)));

    // copy the old values in the new vector
    for (uint64_t a = 0; a < numEPNS; a++) {
      // check if EPN did get lost.
      if (history[prevKeyInt].at(a).ts < tooOld) {
        history[key].at(a).ts = 0;
        history[key].at(a).memVal = 0;
      } else {
        history[key].at(a).ts = history[prevKeyInt].at(a).ts;
        history[key].at(a).memVal = history[prevKeyInt].at(a).memVal;
      }
    }

    // overwrite with the current values.
    history[key].at(epnId - 1).ts = key;
    history[key].at(epnId - 1).memVal = myMem;

    // }
  }
  // check if the history should be trimmed
  // get first key
  auto firstKey = history.begin();
  tooOld = (*firstKey).first;
  while (key >= (tooOld + historyMaxMs)) {
    // delete the first key.
    history.erase(firstKey);
    auto newFirstKey = history.begin();
    // update tooOld integer
    tooOld = newFirstKey->first;
  }
  // write history to file every minute.
  if (key >= (keyForToFile + historyMaxMs)) {
    toFile();
    keyForToFile = key;
  }
}

void scheduler::toFile() {
  for (auto a = history.begin(); a != history.end(); a++) {
    for (uint64_t i = 0; i < (numEPNS - 1); i++) {
      // LOG(INFO)<< "inside FILE LOOP and writing to history: "<< history[(*a).first].at(i).memVal;
      heatdata1 << history[(*a).first].at(i).memVal << ", ";
    }
    // LOG(INFO)<< "outer For loop and writing the last value of the history to file which
    // is"<<history[(*a).first].at(numEPNS-1).memVal;
    heatdata1 << history[(*a).first].at(numEPNS - 1).memVal << endl;
  }
}

std::vector<uint64_t> scheduler::generateSchedule() {
  // LOG(INFO)<< "inside generating schedule";
  vector<int> temporaryStorage =
      generateArray1();  // pointer to array of amount of EPNs which we need to store the memory size of the valid EPNs
  // LOG(INFO)<<"generated temporary Storage vector";
  std::vector<uint64_t> desFLPsPointer;
  // LOG(INFO)<<"vector desFLPsPointer generated";
  // printfreeSlots(temporaryStorage, 1);
  // using maxSearchFunction the size of "amountEPNs" times to get the Ids of the EPNs with the highest memory
  // capacities
  for (uint64_t i = 0; i < amountEPNs; i++) {
    // LOG(INFO)<<"Inside for loop for finding  the max value the "<<i<<". time";
    int maxIndex = maxSearch(temporaryStorage);
    desFLPsPointer.push_back(maxIndex);  // index of the EPNs with most memory capacity
  }

  // now decrement resources of selected EPNs!
  // so they are not use in the next schedule again
  auto latestKey = history.rbegin();
  for (unsigned i = 0; i < desFLPsPointer.size(); i++) {
    if (desFLPsPointer[i] == -1) {
      continue;
    }

    // just to check
    uint64_t realIndex = desFLPsPointer[i] - 1;
    if (latestKey->second.at(realIndex).memVal <= 0) {
      LOG(WARNING) << "ERROR: EPN selected for schedule but does not have memory slots!";
      LOG(WARNING) << "memory slots not decreased";
    } else {
      latestKey->second.at(realIndex).memVal -= 1;
      // LOG(INFO)<< "decreased memory slots in history";
    }
  }

  return desFLPsPointer;
}

vector<int> scheduler::generateArray1() {
  vector<int> temporaryStorage(numEPNS, -1 /* def value */);

  auto latestKey = history.rbegin();
  // checking whether the EPNs are valid
  for (uint64_t i = 0; i < numEPNS; i++) {
    if (latestKey->second.at(i).ts + 5000 >= latestKey->first) {
      // writing the memory values of the valid EPns in array
      temporaryStorage[i] = latestKey->second.at(i).memVal;
    } else {  // setting the other values to -1
      temporaryStorage[i] = -1;
      latestKey->second.at(i).memVal = -1;  // mark them as failed
    }
  }

  return temporaryStorage;
}

int scheduler::maxSearch(vector<int>& arr) {
  int max = arr[0];
  int index = 0;
  for (uint64_t i = 1; i < numEPNS; i++) {
    if (max < arr[i]) {
      max = arr[i];
      index = i;
    }
  }

  if (arr[index] == 0 || arr[index] == -1) {
    arr[index] = -1;
    return -1;
  } else {
    arr[index] = -1;
    return index + 1;
  }
}

void scheduler::printVecFLP(std::vector<uint64_t> a) {
  for (int b = 0; b != a.size(); b++) {
    cout << "Printing the vector for the flps. number: " << b + 1 << " goes to: " << a.at(b) << endl;
  }
}

void scheduler::printfreeSlots(int arr[], int length) {
  for (int n = 0; n < length; ++n) cout << "free Slots: " << arr[n] << ' ';
  cout << '\n';
}

static std::mutex sched_send_mutex;

void scheduler::sender(std::vector<uint64_t>* vec, uint64_t* num, uint64_t* numE, uint64_t* schedNum) {
  const vector<uint64_t> lVec = *vec;
  assert(lVec.size() == amountEPNs);
  vec->clear();

  for (vector<uint64_t>::const_iterator iter = lVec.begin(); iter != lVec.end(); ++iter) {
    LOG(INFO) << "Schedule number " << *schedNum << " sent Id of epn which is: " << *iter;
  }

  {
    std::lock_guard<std::mutex> guard(sched_send_mutex);

    for (uint64_t i = 0; i < numFLPS; i++) {
      auto& mySendingChan = GetChannel("schedflp", i);

      FairMQMessagePtr message = mySendingChan.NewMessage((sizeof(uint64_t)) * lVec.size());

      std::memcpy(message->GetData(), lVec.data(), message->GetSize());
      mySendingChan.Send(message);
    }
  }
}

int scheduler::availableEpns(const vector<int>& arr, uint64_t siz) const {
  int c = 0;
  for (uint64_t i = 0; i < siz; i++) {
    if (arr[i] > 0) {
      c++;
    }
  }
  return c;
}

int scheduler::EpnsInSchedule(int avaiable, uint64_t siz) {
  if (avaiable < siz) {
    return avaiable;
  } else {
    return siz;
  }
}

std::vector<uint64_t> scheduler::simpleRRSched(int mm) {
  LOG(INFO) << "before creating vector";
  std::vector<uint64_t> roundr;
  LOG(INFO) << "just created vector";
  auto prevKey = history.rbegin();
  LOG(INFO) << "got the last key";
  uint64_t prevKeyInt = prevKey->first;
  LOG(INFO) << "made the last key to an integer";
  for (int j = mm; j < (mm + amountEPNs); j++) {
    LOG(INFO) << "inside the for loop";
    if (j <= numEPNS) {
      if (history[prevKeyInt].at(j - 1).memVal == 0) {  // case that it's either zero or broken
        LOG(INFO) << "no memory capacity";
        roundr.push_back(-1);
        LOG(INFO) << "inserted -1 in the vector";
      } else {
        LOG(INFO) << "memory capacity";
        roundr.push_back(j);
        epnsInScheduleRR++;
        LOG(INFO) << "pushed back: " << j;
      }
    } else {
      int k = j % numEPNS;
      if (history[prevKeyInt].at(k - 1).memVal == 0) {  // case that it's either zero or broken
        LOG(INFO) << "no memory capacity";
        roundr.push_back(-1);
        LOG(INFO) << "inserted -1 in the vector";
      } else {
        LOG(INFO) << "memory capacity";
        roundr.push_back(k);
        epnsInScheduleRR++;
        LOG(INFO) << "pushed back: " << k;
      }
    }
  }
  return roundr;
}

scheduler::~scheduler() {}

}  // namespace SCHEDULER_FLP_EPN
