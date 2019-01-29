/**
 * flp.cpp
 *
 */

#include <chrono>
#include <memory>  // unique_ptr
#include <thread>  // this_thread::sleep_for

#include <fstream>
#include <sstream>
#include <string>
#include "FairMQPoller.h"
#include "Message.h"
#include "flp.h"

using namespace std;

namespace example_SCHEDULER_FLP_EPN {

flp::flp()
    : arrayofEpns(),
      msBetweenSubtimeframes(25),
      amountEPNs(0),
      numEPNS(0),
      socket(0),
      myId(0),
      sTF(0),
      schedNum(0),
      startTime(0),
      programTime(0),
      amountOfLostTfs1() {}

void flp::InitTask() {
  amountEPNs = fConfig->GetValue<int>("amountEPNs");
  numEPNS = fConfig->GetValue<uint64_t>("numEPNS");
  socket = fConfig->GetValue<int>("socket");
  myId = fConfig->GetValue<int>("myId");
  programTime = fConfig->GetValue<uint64_t>("programTime");
  arrayofEpns = new int[amountEPNs];
  startTime = getHistKey();
}

static uint64_t getCurrentMs() {
  // get the current time
  const auto time = std::chrono::high_resolution_clock::now();
  // get the time from THEN to now
  auto duration = time.time_since_epoch();
  const std::uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

  return millis;
}

void flp::Run() {
  while (CheckCurrentState(RUNNING))  //
  {
    auto &myRecvChan = GetChannel("schedflp");
    // I expect this kind of message
    std::vector<uint64_t> msgIReceive(amountEPNs, 0);

    // receive a message
    FairMQMessagePtr aMessage = myRecvChan.NewMessage();
    if (myRecvChan.Receive(aMessage) != ((sizeof(uint64_t)) * amountEPNs)) {
      LOG(ERROR) << "Error while receiving EPN updates.";
      return;
    }

    // get the pointer of the message.
    std::memcpy(msgIReceive.data(), aMessage->GetData(), ((sizeof(uint64_t)) * amountEPNs));
    if (aMessage->GetSize() == ((sizeof(uint64_t)) * amountEPNs)) {
      schedNum++;
      int i = 0;

      for (vector<uint64_t>::const_iterator iter = msgIReceive.begin(); iter != msgIReceive.end(); ++iter) {
        arrayofEpns[i] = *iter;
        i++;
      }

      if ((arrayofEpns[0] == 3) && (arrayofEpns[1] == 3)) {
        for (int i = 0; i < numEPNS; i++) {
          auto &mySendingChan = GetChannel("data", i);
          FLPtoEPN MsgFlpEpn;
          MsgFlpEpn.IdOfFlp = myId;
          MsgFlpEpn.sTF = 1;
          MsgFlpEpn.schedNum = -1;
          FairMQMessagePtr message = mySendingChan.NewMessage(sizeof(FLPtoEPN));
          std::memcpy(message->GetData(), &MsgFlpEpn, sizeof(FLPtoEPN));

          mySendingChan.Send(message);
        }

        ofstream amountOfLostTfs;
        amountOfLostTfs.open("amountOfLostTfs.txt." + to_string(myId), std::ios_base::trunc);
        amountOfLostTfs << amountOfLostTfs1.rdbuf();

        amountOfLostTfs.flush();
        amountOfLostTfs.close();

        // make sure all messages are sent
        std::this_thread::sleep_for(std::chrono::seconds(long(5)));

        LOG(INFO) << "TERMINATING PROGRAM NOW!";
        exit(0);
        return;
      }

      LOG(INFO) << "time now: " << getHistKey();
      const auto startSendTime = getCurrentMs();
      for (int i = 0; i < amountEPNs; i++) {
        sTF++;
        int c = arrayofEpns[i];
        if (c >= 0) {
          auto &mySendingChan = GetChannel("data", (c - 1));
          // the message I want to send
          FLPtoEPN MsgFlpEpn;
          MsgFlpEpn.IdOfFlp = myId;
          MsgFlpEpn.sTF = sTF;
          MsgFlpEpn.schedNum = schedNum;

          FairMQMessagePtr message = mySendingChan.NewMessage(sizeof(FLPtoEPN));
          std::memcpy(message->GetData(), &MsgFlpEpn, sizeof(FLPtoEPN));

          mySendingChan.Send(message);

          LOG(info) << "Sent to Epn \"" << c << " and subtimeframe: " << MsgFlpEpn.sTF
                    << " and my Id is: " << MsgFlpEpn.IdOfFlp << "\"";
          if ((getCurrentMs() - startSendTime) / msBetweenSubtimeframes > i) {
            continue;  // send more
          } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(msBetweenSubtimeframes - 5));  // wait 25 - 1 ms.
          }
        }

        else {
          // LOG(INFO)<< "Unfortunately there is no Epn guaranteeing the memory capacity";
          amountOfLostTfs1 << sTF << endl;
        }
      }
    }
  }  // while (RUNNING)
}

uint64_t flp::getHistKey() {
  // get the current time
  const auto time = std::chrono::high_resolution_clock::now();
  // get the time from THEN to now
  auto duration = time.time_since_epoch();
  const std::uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

  // std::cout << "Milliseconds : " << millis << std::endl;
  // determine the interval (key for the hist map)
  const std::uint64_t intKey = millis / 1000 * 1000;
  // cout<< "histKey : " << intKey << endl;

  return intKey;
}

flp::~flp() {
  if (arrayofEpns) delete[] arrayofEpns;
}
}
