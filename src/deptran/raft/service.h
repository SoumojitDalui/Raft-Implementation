#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "server.h"
#include "macros.h"

class SimpleCommand;
namespace janus {

class TxLogServer;
class RaftServer;
class RaftServiceImpl : public RaftService {
 public:
  RaftServer* svr_;
  RaftServiceImpl(TxLogServer* sched);

  RpcHandler(RequestVote, 6, const uint64_t&, term, const uint64_t&, candidateId, const uint64_t&, lastLogIndex, const uint64_t&, lastLogTerm, uint64_t*, retTerm, bool_t*, voteGranted) {
    *retTerm = 0;
    *voteGranted = false;
  }

  RpcHandler(AppendEntries, 10, const uint64_t&, term, const uint64_t&, leaderId, const uint64_t&, prevLogIndex, const uint64_t&, prevLogTerm, const uint64_t&, entryIndex, const uint64_t&, entryTerm, const MarshallDeputy&, entry, const uint64_t&, leaderCommit, uint64_t*, retTerm, bool_t*, success) {
    *retTerm = 0;
    *success = false;
  }

  RpcHandler(HelloRpc, 2, const string&, req, string*, res) {
    *res = "error"; 
  };

};

} // namespace janus
