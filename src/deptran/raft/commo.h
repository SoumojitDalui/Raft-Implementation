#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"

namespace janus {

class TxData;

class RaftCommo : public Communicator {

 public:
  RaftCommo() = delete;
  RaftCommo(PollMgr*);

  shared_ptr<IntEvent>
  SendRequestVote(parid_t par_id, siteid_t site_id, uint64_t term, uint64_t candidateId, uint64_t lastLogIndex, uint64_t lastLogTerm, uint64_t *retTerm, bool_t *voteGranted);

  shared_ptr<IntEvent>
  SendAppendEntries(parid_t par_id, siteid_t site_id, uint64_t term, uint64_t leaderId, uint64_t prevLogIndex, uint64_t prevLogTerm, uint64_t entryIndex, uint64_t entryTerm, const MarshallDeputy entry, uint64_t leaderCommit, uint64_t *retTerm, bool_t *success);

  shared_ptr<IntEvent> 
  SendString(parid_t par_id, siteid_t site_id, const string& msg, string* res);

  /* Do not modify this class below here */

  friend class FpgaRaftProxy;
 public:
#ifdef RAFT_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
#endif
};

} // namespace janus
