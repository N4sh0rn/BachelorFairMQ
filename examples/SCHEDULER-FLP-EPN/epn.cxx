/**
 * epn.cxx
 *
 */

#include "epn.h"
#include <time.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include "FairMQPoller.h"
#include "Message.h"

using namespace std;

namespace example_SCHEDULER_FLP_EPN {

epn::epn()
    : Id(0),
      freeSlots(0),
      maxSlots(),
      numEPNS(),
      numFLPS(0),
      rcvdSTFs(),
      it(),
      sTF(1),
      procTime(30),
      procDev(5),
      startTime(0),
      timeBetweenTf(0),
      programTime(0),
      intMs(1000),
      receptionOfTf1(),
      processingTime1(),
      numberOfLostTfs1()

{
  static_assert(std::is_pod<EPNtoScheduler>::value == true, "my struct  is not pod");
}

void epn::InitTask() {
  timeBetweenTf = getHistKey();
  startTime = getHistKey();
  Id = fConfig->GetValue<uint64_t>("myId");
  maxSlots = fConfig->GetValue<uint64_t>("maxSlots");
  freeSlots = maxSlots;
  numEPNS = fConfig->GetValue<uint64_t>("numEPNS");
  numFLPS = fConfig->GetValue<uint64_t>("numFLPS");
  std::thread th1 = std::thread(&epn::send, this);
  th1.detach();
}

bool epn::ConditionalRun() { return receive(); }

uint64_t epn::getHistKey() {
  // get the current time
  const auto time = std::chrono::high_resolution_clock::now();
  // get the time from THEN to now
  auto duration = time.time_since_epoch();
  const std::uint64_t intKey = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  // determine the interval (key for the hist map)
  // const std::uint64_t intKey = millis / intMs * intMs;
  return intKey;
}

bool epn::receive() {
  FairMQPollerPtr poller(NewPoller("data"));
  poller->Poll(100000);

  unsigned num_received = 0;

  for (uint64_t i = 0; i < numFLPS; i++) {
    if (!poller->CheckInput("data", i)) {
      continue;
    }

    auto &myRecvChan = GetChannel("data", i);

    FLPtoEPN messagei;

    FairMQMessagePtr aMessage = myRecvChan.NewMessage();

    if (sizeof(FLPtoEPN) != myRecvChan.Receive(aMessage)) {
      LOG(ERROR) << "Bad Message from FLP" << i + 1;
      continue;
    }

    num_received++;

    std::memcpy(&messagei, aMessage->GetData(), sizeof(FLPtoEPN));

    if (messagei.IdOfFlp != (i + 1)) {
      LOG(ERROR) << "Bad Message ID from FLP" << i + 1 << " != " << messagei.IdOfFlp;
      continue;
    }

    if (messagei.sTF == 1 && messagei.schedNum == -1) {
      ofstream receptionOfTf, processingTime, numberOfLostTfs;

      receptionOfTf.open("TimebetweenReceptionOfTf.txt." + to_string(Id), std::ios_base::trunc);
      processingTime.open("processingTime.txt." + to_string(Id), std::ios_base::trunc);
      // numberOfLostTfs.open("numberOfLostTfs.txt."+to_string(Id),

      receptionOfTf << receptionOfTf1.rdbuf();
      processingTime << processingTime1.rdbuf();

      receptionOfTf.flush();
      processingTime.flush();

      receptionOfTf.close();
      processingTime.close();

      // numberOfLostTfs<<numberOfLostTfs1.rdbuf();

      std::this_thread::sleep_for(std::chrono::seconds(5));
      LOG(INFO) << "TERMINATING PROGRAM NOW!";
      exit(0);
      return false;  // stop the FairMQ device
    }

    rcvdSTFs[messagei.sTF]++;
    if (rcvdSTFs[messagei.sTF] == numFLPS) {
      receptionOfTf1 << (getHistKey() - timeBetweenTf) << skipws << endl;
      timeBetweenTf = getHistKey();
      // LOG(info) << "Epn: "<< Id<<" received data from FLP number: " << messagei.IdOfFlp << " and sTF number is "<<
      // messagei.sTF;
    }
    /*
    for( it=rcvdSTFs.begin(); it!=rcvdSTFs.end(); it++){
      cout<< it->first<<" =>"<< it->second<<endl;
    }
    */
    assert(rcvdSTFs[messagei.sTF] <= numFLPS);

    if (rcvdSTFs[messagei.sTF] == numFLPS) {
      if (freeSlots > 0) {
        freeSlots--;
        float x = getDelay();
        LOG(INFO) << "Delay work for: " << x << " seconds for TFid: " << messagei.sTF << " and my ID is: " << Id
                  << "and this is schedule nr: " << messagei.schedNum;
        std::thread t3(&epn::MyDelayedFun, this, x);
        t3.detach();
      } else {
        LOG(INFO) << "INFORMATION LOST DUE TO OVERCAPACITY AND MY ID IS: " << Id
                  << "AND THIS IS SCHEDULE NR.: " << messagei.schedNum;
        numberOfLostTfs1 << messagei.sTF << endl;
      }
    }
  }

  LOG(INFO) << "Received " << num_received << " STFs";

  return true;
}

void epn::send() {
  // change the sending
  auto &mySendingChan = GetChannel("epnsched");

  unsigned sent_updates = 0;

  while (true) {

    if (!CheckCurrentState("RUNNING")) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    EPNtoScheduler myMsg;
    myMsg.Id = Id;
    myMsg.freeSlots = freeSlots.load();
    myMsg.numEPNs = numEPNS;

    FairMQMessagePtr message = mySendingChan.NewMessage(sizeof(EPNtoScheduler));
    std::memcpy(message->GetData(), &myMsg, sizeof(EPNtoScheduler));
    mySendingChan.Send(message);

    // LOG(INFO)<<"sent ID: "<<myMsg.Id<<" and amount of free slots
    // "<<myMsg.freeSlots<< " general amount of EPNs: "  <<myMsg.numEPNs <<
    // endl;

    if (++sent_updates % 50 == 0) {
      LOG(INFO) << "Free slot in update (" << sent_updates << ") sent: " << myMsg.freeSlots;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(long(250)));
  }

  LOG(INFO) << "Exiting sender update thread.";
}

void epn::MyDelayedFun(float delayWork) {
  //(*memory)--;
  // LOG(INFO)<<"amount of memory slots after decrementing: "<<*memory<<endl;
  processingTime1 << delayWork << endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(long(delayWork * 1000)));
  freeSlots++;

  LOG(INFO) << "TF processed, new free slot count: " << freeSlots;
}

float epn::getDelay() {
  static std::random_device generator;
  std::normal_distribution<float> distribution(procTime, procDev);
  float delay = distribution(generator);
  return delay;
}

epn::~epn() {}
}
