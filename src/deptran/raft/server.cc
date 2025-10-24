#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"
#include "raft_rpc.h"
#include "config.h"
#include "macros.h"
#include <algorithm>

namespace janus
{
  void RaftServer::SnapshotLeaderLog(uint64_t *termNow, uint64_t *leaderCommit, uint64_t *lastIndex, uint64_t *prevTerm)
  {
    std::lock_guard<std::recursive_mutex> lk(mtx_);
    *termNow = current_term_;
    *leaderCommit = commit_index_;
    *lastIndex = (log_.size() > 0) ? (uint64_t)log_.size() - 1 : 0;
    *prevTerm = (*lastIndex > 0) ? log_[*lastIndex].term : 0;
  }

  void RaftServer::StepDownIfHigherTerm(uint64_t retTerm)
  {
    if (retTerm > current_term_)
    {
      BecomeFollower(retTerm);
    }
  }

  void RaftServer::SendHeartbeatTo(siteid_t target, uint64_t termNow, uint64_t prevIdx, uint64_t prevTerm, uint64_t leaderCommit)
  {
    uint64_t *retTerm = new uint64_t(0);
    bool_t *ok = new bool_t(false);

    // Heartbeat is AppendEntries with no-op payload and entryIndex=0
    // We must send a valid Marshallable due to MarshallDeputy invariants.
    auto noop = std::make_shared<TpcNoopCommand>();
    MarshallDeputy empty_entry(noop);
    auto ev = commo()->SendAppendEntries(partition_id_, target,
                                         termNow,    // term
                                         loc_id_,    // leaderId
                                         prevIdx, prevTerm,
                                         0 /*entryIndex*/, 0 /*entryTerm*/,
                                         empty_entry,
                                         leaderCommit,
                                         retTerm, ok);

    Coroutine::CreateRun([this, ev, retTerm, ok]()
                         {
                          ev->Wait(200000);
                          std::lock_guard<std::recursive_mutex> lk(this->mtx_);
                          if (ev->status_ != Event::TIMEOUT) {
                            this->StepDownIfHigherTerm(*retTerm);
                          }
                          delete retTerm;
                          delete ok; });
  }

  uint64_t RaftServer::NowUs() const
  {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + ts.tv_nsec / 1000ull;
  }

  void RaftServer::ResetElectionDeadline()
  {
    rng_seed_ = rng_seed_ * 2862933555777941757ULL + 3037000493ULL;
    uint64_t span = (kElectionMaxUs > kElectionMinUs) ? (kElectionMaxUs - kElectionMinUs) : 0;
    uint64_t r = span ? ((rng_seed_ >> 33) % (span + 1)) : 0;
    election_deadline_us_ = NowUs() + kElectionMinUs + r;
  }

  void RaftServer::BecomeFollower(uint64_t new_term)
  {
    role_ = FOLLOWER;
    if (new_term > current_term_)
    {
      current_term_ = new_term;
      voted_for_ = -1;
    }
    ResetElectionDeadline();
  }

  void RaftServer::BecomeCandidate()
  {
    role_ = CANDIDATE;
    current_term_ += 1;
    voted_for_ = loc_id_;
    ResetElectionDeadline();
  }

  void RaftServer::BecomeLeader()
  {
    role_ = LEADER;
    last_heartbeat_us_ = 0;

    uint64_t last_index = (log_.size() > 0) ? (uint64_t)log_.size() - 1 : 0;
    next_index_.clear();
    match_index_.clear();

    // leader counts itself
    match_index_[loc_id_] = last_index;

    auto it = commo()->rpc_par_proxies_.find(partition_id_);
    if (it != commo()->rpc_par_proxies_.end())
    {
      for (auto &pr : it->second)
      {
        siteid_t target = pr.first;
        if (target == loc_id_)
          continue;
        next_index_[target] = last_index + 1;
        match_index_[target] = 0;
        Coroutine::CreateRun([this, target]
                             { this->ReplicateFollower(target); });
      }
    }
    // Do not send immediate heartbeats; replication loops will kick in
    // and act as heartbeats while catching followers up.
  }

  void RaftServer::ApplyCommitted()
  {
    while (last_applied_ < commit_index_)
    {
      last_applied_++;
      if (log_.size() > last_applied_ && log_[last_applied_].cmd && app_next_)
      {
        app_next_(*log_[last_applied_].cmd);
      }
    }
  }

  void RaftServer::ElectionLoop()
  {
    {
      std::lock_guard<std::recursive_mutex> lk(mtx_);
      if (log_.empty())
        log_.push_back(LogEntry{});
      struct timespec ts;
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
      rng_seed_ = ts.tv_nsec ^ ((uint64_t)loc_id_ << 20);
      ResetElectionDeadline();
    }

    while (true)
    {
      Reactor::CreateSpEvent<TimeoutEvent>(20000)->Wait();
      std::lock_guard<std::recursive_mutex> lk(mtx_);
      const uint64_t now = NowUs();

      if (role_ != LEADER && now >= election_deadline_us_)
      {
        BecomeCandidate();
        uint64_t last_index = (log_.size() > 0) ? (uint64_t)log_.size() - 1 : 0;
        uint64_t last_term = (last_index > 0) ? log_[last_index].term : 0;
        int majority = Config::GetConfig()->GetPartitionSize(partition_id_) / 2 + 1;
        auto votes = std::make_shared<int>(1); // self-vote
        uint64_t my_term = current_term_;

        auto it = commo()->rpc_par_proxies_.find(partition_id_);
        if (it != commo()->rpc_par_proxies_.end())
        {
          for (auto &pr : it->second)
          {
            siteid_t target = pr.first;
            if (target == loc_id_)
              continue;

            auto ret_term = new uint64_t(0);
            auto granted = new bool_t(false);

            auto ev = commo()->SendRequestVote(partition_id_, target,
                                               my_term,    // term
                                               loc_id_,    // candidateId
                                               last_index, // lastLogIndex
                                               last_term,  // lastLogTerm
                                               ret_term, granted);

            Coroutine::CreateRun([this, ev, ret_term, granted, votes, majority, my_term]()
                                 {
  ev->Wait(200000); // 200ms to avoid spurious timeouts
  std::lock_guard<std::recursive_mutex> lk(this->mtx_);
  if (ev->status_ != Event::TIMEOUT) {
    if (*ret_term > this->current_term_) {
      this->BecomeFollower(*ret_term);
    } else if (this->role_ == CANDIDATE && this->current_term_ == my_term && *granted) {
      (*votes)++;
      if ((*votes) >= majority) this->BecomeLeader();
    }
  }
  delete ret_term;
  delete granted; });
          }
        }
        ResetElectionDeadline();
      }

      if (role_ == LEADER && (NowUs() - last_heartbeat_us_) >= HEARTBEAT_INTERVAL)
      {
        HeartbeatOnce();
      }
    }
  }

  void RaftServer::HeartbeatOnce()
  {
    last_heartbeat_us_ = NowUs();

    auto peers = commo()->rpc_par_proxies_.find(partition_id_);
    if (peers == commo()->rpc_par_proxies_.end())
      return;

    uint64_t term_now, leader_commit, last_index, prev_term;
    SnapshotLeaderLog(&term_now, &leader_commit, &last_index, &prev_term);

    {
      std::lock_guard<std::recursive_mutex> lk(mtx_);
      if (role_ != LEADER)
        return;
    }

    for (auto &pr : peers->second)
    {
      siteid_t target = pr.first;
      if (target == loc_id_)
        continue;
      // Skip heartbeat if follower is behind; replication loop will send AE
      bool up_to_date = true;
      {
        std::lock_guard<std::recursive_mutex> lk(mtx_);
        auto itn = next_index_.find(target);
        uint64_t ni = (itn == next_index_.end()) ? (last_index + 1) : itn->second;
        up_to_date = (ni > last_index);
      }
      if (!up_to_date) continue;
      SendHeartbeatTo(target, term_now, last_index, prev_term, leader_commit);
    }
  }

  void RaftServer::OnRequestVote(uint64_t term, uint64_t candidateId, uint64_t lastLogIndex, uint64_t lastLogTerm, uint64_t *retTerm, bool_t *voteGranted)
  {
    std::lock_guard<std::recursive_mutex> lk(mtx_);
    if (term > current_term_)
    {
      BecomeFollower(term);
    }
    *retTerm = current_term_;
    *voteGranted = false;

    if (term < current_term_)
    {
      return;
    }

    uint64_t my_last_index = (log_.size() > 0) ? (uint64_t)log_.size() - 1 : 0;
    uint64_t my_last_term = (my_last_index > 0) ? log_[my_last_index].term : 0;

    bool up_to_date = (lastLogTerm > my_last_term) ||
                      (lastLogTerm == my_last_term && lastLogIndex >= my_last_index);

    if ((voted_for_ == -1 || voted_for_ == (int64_t)candidateId) && up_to_date)
    {
      voted_for_ = (int64_t)candidateId;
      ResetElectionDeadline();
      *voteGranted = true;
    }
  }

  void RaftServer::OnAppendEntriesOne(uint64_t term, uint64_t leaderId, uint64_t prevLogIndex, uint64_t prevLogTerm, uint64_t entryIndex, uint64_t entryTerm, std::shared_ptr<Marshallable> entry, uint64_t leaderCommit, uint64_t *retTerm, bool_t *success)
  {
    (void)leaderId;
    std::lock_guard<std::recursive_mutex> lk(mtx_);
    if (term > current_term_)
    {
      BecomeFollower(term);
    }
    *retTerm = current_term_;
    if (term < current_term_)
    {
      *success = false;
      return;
    }

    role_ = FOLLOWER;
    ResetElectionDeadline();

    uint64_t last_index = (log_.size() > 0) ? (uint64_t)log_.size() - 1 : 0;
    if (prevLogIndex > last_index)
    {
      *success = false;
      return;
    }
    if (prevLogIndex > 0 && log_[prevLogIndex].term != prevLogTerm)
    {
      *success = false;
      return;
    }

    if (entryIndex > 0)
    {
      if (entryIndex <= last_index && log_[entryIndex].term != entryTerm)
      {
        log_.resize(entryIndex);
        last_index = entryIndex - 1;
      }
      if (entryIndex == last_index + 1)
      {
        log_.push_back(LogEntry{entryTerm, entry});
        last_index = entryIndex;
      }
    }

    if (leaderCommit > commit_index_)
    {
      commit_index_ = std::min(leaderCommit, last_index);
      ApplyCommitted();
    }
    *success = true;
  }

  // OnHeartBeat removed; heartbeats use OnAppendEntriesOne with entryIndex=0

  RaftServer::RaftServer(Frame *frame)
  {
    frame_ = frame;
    /* Your code here for server initialization. Note that this function is
       called in a different OS thread. Be careful about thread safety if
       you want to initialize variables here. */
  }

  RaftServer::~RaftServer()
  {
    /* Your code here for server teardown */
  }

  void RaftServer::KickReplication(uint64_t from_index)
  {
  }

  void RaftServer::ReplicateFollower(siteid_t target)
  {
    while (true)
    {
      // throttle loop a bit to avoid busy spin
      Reactor::CreateSpEvent<TimeoutEvent>(40000)->Wait(); // 40ms

      // snapshot state under lock
      uint64_t term_now, commit_now, li, ni;
      {
        std::lock_guard<std::recursive_mutex> lk(mtx_);
        if (role_ != LEADER)
          return;
        term_now = current_term_;
        commit_now = commit_index_;
        li = (log_.size() > 0) ? (uint64_t)log_.size() - 1 : 0;
        ni = next_index_[target];
      }

      // follower up-to-date: central HeartbeatOnce() will handle heartbeats
      if (ni > li)
      {
        continue;
      }

      // build single-entry AppendEntries for index ni
      uint64_t prev_idx = (ni > 0) ? (ni - 1) : 0;
      uint64_t prev_term;
      uint64_t entry_idx = ni;
      uint64_t entry_term;
      std::shared_ptr<Marshallable> entry_sp;
      {
        std::lock_guard<std::recursive_mutex> lk(mtx_);
        prev_term = (prev_idx > 0) ? log_[prev_idx].term : 0;
        if (entry_idx < log_.size())
        {
          entry_term = log_[entry_idx].term;
          entry_sp = log_[entry_idx].cmd;
        }
        else
        {
          // nothing to send yet
          continue;
        }
      }

      MarshallDeputy md(entry_sp);
      auto ret_term = new uint64_t(0);
      auto success = new bool_t(false);

      auto ev = commo()->SendAppendEntries(partition_id_, target,
                                           term_now, // leader term
                                           loc_id_,  // leader id
                                           prev_idx, prev_term,
                                           entry_idx, entry_term,
                                           md, commit_now,
                                           ret_term, success);

      // capture term_now and entry_idx for term and index checks
      Coroutine::CreateRun([this, target, ev, ret_term, success, entry_idx, term_now]()
                           {
  ev->Wait(300000); // 300ms per follower AE
  std::lock_guard<std::recursive_mutex> lk(this->mtx_);
  if (ev->status_ == Event::TIMEOUT) {
    delete ret_term;
    delete success;
    return;
  }

  // step down if observed higher term
  this->StepDownIfHigherTerm(*ret_term);
  // if no longer the same-term leader, stop acting on this result
  if (this->role_ != LEADER || this->current_term_ != term_now) {
    delete ret_term;
    delete success;
    return;
  }

  if (*success) {
    // advance next/match for this follower
    this->next_index_[target] = entry_idx + 1;
    this->match_index_[target] = entry_idx;

    // try to advance commitIndex using majority and current-term rule
    int majority = Config::GetConfig()->GetPartitionSize(this->partition_id_) / 2 + 1;
    uint64_t last_idx = (this->log_.size() > 0) ? (uint64_t)this->log_.size() - 1 : 0;
    for (uint64_t N = this->commit_index_ + 1; N <= last_idx; ++N) {
      if (this->log_[N].term != this->current_term_) continue;
      int count = 1; // leader itself
      for (auto& pr : this->match_index_) {
        if (pr.first == this->loc_id_) continue;
        if (pr.second >= N) count++;
      }
      if (count >= majority) {
        this->commit_index_ = N;
      }
    }
    this->ApplyCommitted();
  } else {
    // conflict: back off next_index_
    if (this->next_index_[target] > 1) {
      this->next_index_[target]--;
    }
  }

  delete ret_term;
  delete success; });
    }
  }

  void RaftServer::Setup()
  {
    /* Your code here for server setup. Due to the asynchronous nature of the
       framework, this function could be called after a RPC handler is triggered.
       Your code should be aware of that. This function is always called in the
       same OS thread as the RPC handlers. */
    Coroutine::CreateRun([this]()
                         { this->ElectionLoop(); });
  }

  bool RaftServer::Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    uint64_t new_index = 0;
    {
      std::lock_guard<std::recursive_mutex> lk(mtx_);
      if (term)
        *term = current_term_;
      if (role_ != LEADER)
      {
        if (index)
          *index = 0;
        return false;
      }
      // 1-based log with a dummy at index 0
      new_index = (log_.size() > 0) ? (uint64_t)log_.size() : 1;
      log_.push_back(LogEntry{current_term_, cmd});
      if (index)
        *index = new_index;
    }
    // Nudge replication; per-follower loops will pick this up.
    KickReplication(new_index);
    return true;
  }

  void RaftServer::GetState(bool *is_leader, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    std::lock_guard<std::recursive_mutex> lk(mtx_);
    if (is_leader)
      *is_leader = (role_ == LEADER);
    if (term)
      *term = current_term_;
  }

  void RaftServer::SyncRpcExample()
  {
    /* This is an example of synchronous RPC using coroutine; feel free to
       modify this function to dispatch/receive your own messages.
       You can refer to the other function examples in commo.h/cc on how
       to send/recv a Marshallable object over RPC. */
    Coroutine::CreateRun([this]()
                         {
    string res;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) {
      Log_info("timeout happens");
    } else {
      Log_info("rpc response is: %s", res.c_str()); 
    } });
  }

  /* Do not modify any code below here */

  void RaftServer::Disconnect(const bool disconnect)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    verify(disconnected_ != disconnect);
    // global map of rpc_par_proxies_ values accessed by partition then by site
    static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
    if (_proxies.find(partition_id_) == _proxies.end())
    {
      _proxies[partition_id_] = {};
    }
    RaftCommo *c = (RaftCommo *)commo();
    if (disconnect)
    {
      verify(_proxies[partition_id_][loc_id_].size() == 0);
      verify(c->rpc_par_proxies_.size() > 0);
      auto sz = c->rpc_par_proxies_.size();
      _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
      c->rpc_par_proxies_ = {};
      verify(_proxies[partition_id_][loc_id_].size() == sz);
      verify(c->rpc_par_proxies_.size() == 0);
    }
    else
    {
      verify(_proxies[partition_id_][loc_id_].size() > 0);
      auto sz = _proxies[partition_id_][loc_id_].size();
      c->rpc_par_proxies_ = {};
      c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
      _proxies[partition_id_][loc_id_] = {};
      verify(_proxies[partition_id_][loc_id_].size() == 0);
      verify(c->rpc_par_proxies_.size() == sz);
    }
    disconnected_ = disconnect;
  }

  bool RaftServer::IsDisconnected()
  {
    return disconnected_;
  }

} // namespace janus
