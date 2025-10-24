
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "macros.h"

namespace janus
{

  RaftCommo::RaftCommo(PollMgr *poll) : Communicator(poll)
  {
  }

  shared_ptr<IntEvent>
  RaftCommo::SendRequestVote(parid_t par_id, siteid_t site_id, uint64_t term, uint64_t candidateId, uint64_t lastLogIndex, uint64_t lastLogTerm, uint64_t *retTerm, bool_t *voteGranted)
  {
    /*
     * Example code for sending a single RPC to server at site_id
     * You may modify and use this function or just use it as a reference
     */
    auto proxies = rpc_par_proxies_[par_id];
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    for (auto &p : proxies)
    {
      if (p.first != site_id)
        continue;
      if (p.first == site_id)
      {
        RaftProxy *proxy = (RaftProxy *)p.second;
        FutureAttr fuattr;
        fuattr.callback = [retTerm, voteGranted, ev](Future *fu)
        {
          fu->get_reply() >> *retTerm;
          fu->get_reply() >> *voteGranted;
          if (ev->status_ != Event::TIMEOUT)
          {
            ev->Set(1);
          }
        };
        Call_Async(proxy, RequestVote, term, candidateId, lastLogIndex, lastLogTerm, fuattr);
      }
    }
    return ev;
  }

  shared_ptr<IntEvent>
  RaftCommo::SendAppendEntries(parid_t par_id, siteid_t site_id, uint64_t term, uint64_t leaderId, uint64_t prevLogIndex, uint64_t prevLogTerm, uint64_t entryIndex, uint64_t entryTerm, const MarshallDeputy entry, uint64_t leaderCommit, uint64_t *retTerm, bool_t *success)
  {
    /*
     * More example code for sending a single RPC to server at site_id
     * You may modify and use this function or just use it as a reference
     */
    auto proxies = rpc_par_proxies_[par_id];
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    for (auto &p : proxies)
    {
      if (p.first != site_id)
        continue;
      if (p.first == site_id)
      {
        RaftProxy *proxy = (RaftProxy *)p.second;
        FutureAttr fuattr;
        fuattr.callback = [retTerm, success, ev](Future *fu)
        {
          fu->get_reply() >> *retTerm;
          fu->get_reply() >> *success;
          if (ev->status_ != Event::TIMEOUT)
          {
            ev->Set(1);
          }
        };
        Call_Async(proxy, AppendEntries, term, leaderId, prevLogIndex, prevLogTerm, entryIndex, entryTerm, entry, leaderCommit, fuattr);
      }
    }
    return ev;
  }

  shared_ptr<IntEvent>
  RaftCommo::SendString(parid_t par_id, siteid_t site_id, const string &msg, string *res)
  {
    auto proxies = rpc_par_proxies_[par_id];
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    for (auto &p : proxies)
    {
      if (p.first == site_id)
      {
        RaftProxy *proxy = (RaftProxy *)p.second;
        FutureAttr fuattr;
        fuattr.callback = [res, ev](Future *fu)
        {
          fu->get_reply() >> *res;
          ev->Set(1);
        };
        /* wrap Marshallable in a MarshallDeputy to send over RPC */
        Call_Async(proxy, HelloRpc, msg, fuattr);
      }
    }
    return ev;
  }

} // namespace janus
