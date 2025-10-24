
#include "../marshallable.h"
#include "service.h"
#include "server.h"

namespace janus
{

  RaftServiceImpl::RaftServiceImpl(TxLogServer *sched)
      : svr_((RaftServer *)sched)
  {
    struct timespec curr_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
    srand(curr_time.tv_nsec);
  }

  void RaftServiceImpl::HandleRequestVote(const uint64_t& term, const uint64_t& candidateId, const uint64_t& lastLogIndex, const uint64_t& lastLogTerm, uint64_t *retTerm, bool_t *voteGranted, rrr::DeferredReply *defer)
  {
    /* Your code here */
    svr_->OnRequestVote(term, candidateId, lastLogIndex, lastLogTerm, retTerm, voteGranted);
    defer->reply();
  }

  void RaftServiceImpl::HandleAppendEntries(const uint64_t& term, const uint64_t& leaderId, const uint64_t& prevLogIndex, const uint64_t& prevLogTerm, const uint64_t& entryIndex, const uint64_t& entryTerm, const MarshallDeputy &entry, const uint64_t& leaderCommit, uint64_t *retTerm, bool_t *success, rrr::DeferredReply *defer)
  {
    /* Your code here */
    MarshallDeputy md = entry;
    auto sp_cmd = md.sp_data_;
    svr_->OnAppendEntriesOne(term, leaderId, prevLogIndex, prevLogTerm, entryIndex, entryTerm, sp_cmd, leaderCommit, retTerm, success);
    defer->reply();
  }

  void RaftServiceImpl::HandleHelloRpc(const string &req, string *res, rrr::DeferredReply *defer)
  {
    /* Your code here */
    Log_info("receive an rpc: %s", req.c_str());
    *res = "world";
    defer->reply();
  }

} // namespace janus
