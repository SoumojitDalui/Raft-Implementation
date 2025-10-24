#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"

namespace janus {

#define HEARTBEAT_INTERVAL 100000

class RaftServer : public TxLogServer {
 public:
  /* Your data here */
  enum Role {FOLLOWER = 0, CANDIDATE = 1, LEADER = 2};
  struct LogEntry {
  uint64_t term{0};
  std::shared_ptr<Marshallable> cmd{nullptr};
  };

  Role role_{FOLLOWER};
  uint64_t current_term_{0};
  int64_t voted_for_{-1};

  std::vector<LogEntry> log_{{}};

  uint64_t commit_index_{0};
  uint64_t last_applied_{0};

  std::unordered_map<siteid_t, uint64_t> next_index_;
  std::unordered_map<siteid_t, uint64_t> match_index_;

  uint64_t last_heartbeat_us_{0};
  uint64_t election_deadline_us_{0};
  uint64_t rng_seed_{0};
  static constexpr uint64_t kElectionMinUs = 400000; // 400ms
  static constexpr uint64_t kElectionMaxUs = 800000; // 800ms

  /* Your functions here */
  uint64_t NowUs() const;
  void ResetElectionDeadline();
  void BecomeFollower(uint64_t new_term);
  void BecomeCandidate();
  void BecomeLeader();
  void ApplyCommitted();
  void ReplicateFollower(siteid_t target);
  void KickReplication(uint64_t from_index);

  void ElectionLoop();
  void HeartbeatOnce();

  void OnRequestVote(uint64_t term, uint64_t candidateId, uint64_t lastLogIndex, uint64_t lastLogTerm, uint64_t* retTerm, bool_t* voteGranted);

  void OnAppendEntriesOne(uint64_t term, uint64_t leaderId, uint64_t prevLogIndex, uint64_t prevLogTerm, uint64_t entryIndex, uint64_t entryTerm, std::shared_ptr<Marshallable> entry, uint64_t leaderCommit, uint64_t* retTerm, bool_t* success);

  void OnHeartBeat(uint64_t term, uint64_t leaderId, uint64_t prevLogIndex, uint64_t prevLogTerm, uint64_t leaderCommit, uint64_t* retTerm, bool_t* success);

 private:
  void SnapshotLeaderLog(uint64_t* termNow, uint64_t* leaderCommit, uint64_t* lastIndex, uint64_t* prevTerm);
  void SendHeartbeatTo(siteid_t target, uint64_t termNow, uint64_t prevIdx, uint64_t prevTerm, uint64_t leaderCommit);
  void StepDownIfHigherTerm(uint64_t reTerm);

  /* do not modify this class below here */

 public:
  RaftServer(Frame *frame) ;
  ~RaftServer() ;

  bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
  void GetState(bool *is_leader, uint64_t *term);

 private:
  bool disconnected_ = false;
	void Setup();

 public:
  void SyncRpcExample();
  void Disconnect(const bool disconnect = true);
  void Reconnect() {
    Disconnect(false);
  }
  bool IsDisconnected();

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
  RaftCommo* commo() {
    return (RaftCommo*)commo_;
  }
};
} // namespace janus
