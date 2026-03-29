// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include <utility>
#include <queue>
#include "rdma.h"
#include "worker.h"
#include "anet.h"
#include "log.h"
#include "ae.h"
#include "client.h"
#include "util.h"
#include "structure.h"
#include "ae.h"
#include "tcp.h"
#include "slabs.h"
#include "zmalloc.h"
#include "kernel.h"
#include "chars.h"
#include "thread.h"  // For RemoteExecutionData and remote_thread_executor

#ifdef NOCACHE
#include "remote_request_nocache.cc"
#else //not NOCACHE
#include "remote_request_cache.cc"
#endif //NOCACHE

#ifdef DHT
void Worker::ProcessRemoteHTable(Client* cli, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "node %d htable (%llu) recv'ed a remote htable request from %d", this->GetWorkerId(), htable, cli->GetWorkerId());
    wr->op = GET_HTABLE_REPLY;
    //epicAssert(htable != nullptr);
    if (htable != nullptr)
        wr->addr = this->ToGlobal(this->htable);
    else
        wr->addr = Gnullptr;
    wr->status = SUCCESS;
    this->SubmitRequest(cli, wr);
    delete wr;
}

void Worker::ProcessHTableReply(Client* cli, WorkRequest* wr) {
    static std::map<uint32_t, GAddr> htableMap;
    int cnt = 0;

    if (cli) {
        htableMap[cli->GetWorkerId()] = wr->addr;
    } else {
        htableMap[this->GetWorkerId()] = ToGlobal(this->htable);
    }

    if (htableMap.size() == this->widCliMapWorker.size()) {
        WorkRequest* pwr;
        if (cli)
            pwr = this->GetPendingWork(wr->id);
        else 
            pwr = wr;
        char* a = (char*)pwr->addr;
        for (auto& p : htableMap) {
            if (p.second != Gnullptr) {
                a += appendInteger(a, p.second);
                cnt++;
            }
        } 
        htableMap.clear();
        //pwr->ptr = (void*)addr;
        pwr->status = SUCCESS;
        if (cli)
            Notify(pwr);
    }

    if (cli)
        delete wr;
}
#endif // DHT

void Worker::ProcessRemoteMemStat(Client* client, WorkRequest* wr) {
  uint32_t qp;
  int wid;
  Size mtotal, mfree;
  vector<Size> stats;
  Split<Size>((char*) wr->ptr, stats);
  epicAssert(stats.size() == wr->size * 4);
  for (int i = 0; i < wr->size; i++) {
    qp = stats[i * 4];
    wid = stats[i * 4 + 1];
    mtotal = stats[i * 4 + 2];
    mfree = stats[i * 4 + 3];

    Client* cli = FindClientWid(wid);
    widCliMapWorker[wid] = cli;
    //		if(GetWorkerId() == wid) {
    //			epicLog(LOG_DEBUG, "Ignore self information");
    //			continue;
    //		}
    cli->lock();
    if (cli) {
      cli->SetMemStat(mtotal, mfree);
    } else {
      epicLog(LOG_WARNING, "worker %d not registered yet", wid);
    }
    cli->unlock();
  }
  /*
   * we don't need wr any more
   */
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteMalloc(Client* client, WorkRequest* wr) {
  void* addr = nullptr;
  if (wr->flag & ALIGNED) {
    addr = sb.sb_aligned_malloc(wr->size);
    epicAssert((uint64_t)addr % BLOCK_SIZE == 0);
  } else {
    addr = sb.sb_malloc(wr->size);
  }
  //FIXME: remove below
  memset(addr, 0, wr->size);
  if (addr) {
    wr->addr = TO_GLOB(addr, base, GetWorkerId());
    wr->status = SUCCESS;
    ghost_size += wr->size;
    if (ghost_size > conf->ghost_th)
      SyncMaster();
    epicLog(LOG_DEBUG,
        "allocated %d at address %lx, base = %lx, wid = %d, gaddr = %lx",
        wr->size, addr, base, GetWorkerId(), wr->addr);
  } else {
    wr->status = ALLOC_ERROR;
  }
  wr->op = MALLOC_REPLY;
  SubmitRequest(client, wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteMallocReply(Client* client, WorkRequest* wr) {
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  if (wr->status) {
    epicLog(LOG_WARNING, "remote malloc error");
  }
  pwr->addr = wr->addr;
  pwr->status = wr->status;
  //	pending_works.erase(wr->id);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  if (Notify(pwr)) {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteGetReply(Client* client, WorkRequest* wr) {
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  pwr->status = wr->status;
  pwr->size = wr->size;

  if (wr->status) {
    epicAssert(!pwr->size);
    epicLog(LOG_WARNING, "cannot get the value for key %ld", wr->key);
  } else {
    memcpy(pwr->ptr, wr->ptr, wr->size);
  }

  if (Notify(pwr)) {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteEvictShared(Client* client, WorkRequest* wr) {
  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry)) {
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in transition state %d",
        directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  directory.Clear(entry, client->ToGlobal(wr->ptr));
  directory.unlock(laddr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteEvictDirty(Client* client, WorkRequest* wr) {
  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry)) {
    //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in transition state %d",
        directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  directory.Clear(entry, client->ToGlobal(wr->ptr));
  directory.unlock(laddr);
  client->WriteWithImm(nullptr, nullptr, 0, wr->id);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRequest(Client* client, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "process remote request %d from worker %d", wr->op,
      client->GetWorkerId());
  epicAssert(wr->wid == 0 || wr->wid == client->GetWorkerId());

  switch (wr->op) {

#ifdef DHT
    case GET_HTABLE: 
    {
        this->ProcessRemoteHTable(client, wr);
        break;
    }
    case GET_HTABLE_REPLY:
    {
        this->ProcessHTableReply(client, wr);
        break;
    }
#endif //DHT

    case FETCH_MEM_STATS_REPLY:
    case BROADCAST_MEM_STATS: 
      {
        ProcessRemoteMemStat(client, wr);
        break;
      }
    case MALLOC: 
      {
        ProcessRemoteMalloc(client, wr);
        break;
      }
    case MALLOC_REPLY: 
      {
        ProcessRemoteMallocReply(client, wr);
        break;
      }
    case FREE: 
      {
        //FIXME: check whether other nodes are sharing this data
        //issue a write request first, and then process the free
        epicAssert(IsLocal(wr->addr));
        Size size = sb.sb_free(ToLocal(wr->addr));
        ghost_size -= size;
        if (labs(ghost_size.load()) > conf->ghost_th)
          SyncMaster();
        delete wr;
        wr = nullptr;
        break;
      }
    case GET_REPLY: 
      {
        ProcessRemoteGetReply(client, wr);
        break;
      }
    case READ:
      {
        ProcessRemoteRead(client, wr);
        break;
      }
    case READ_FORWARD:
    case FETCH_AND_SHARED: 
      {
        ProcessRemoteReadCache(client, wr);
        break;
      }
    case READ_REPLY:
      {
        ProcessRemoteReadReply(client, wr);
        break;
      }
    case WRITE:
    case WRITE_PERMISSION_ONLY: 
      {
        ProcessRemoteWrite(client, wr);
        break;
      }
    case INVALIDATE:
    case FETCH_AND_INVALIDATE:
    case WRITE_FORWARD:
    case INVALIDATE_FORWARD:
    case WRITE_PERMISSION_ONLY_FORWARD: 
      {
        // printf("ProcessRemoteWriteCache\n");
        ProcessRemoteWriteCache(client, wr);
        break;
      }
    case WRITE_REPLY:
      {
        // printf("ProcessRemoteWriteReply\n");
        ProcessRemoteWriteReply(client, wr);
        break;
      }
    case ACTIVE_INVALIDATE:
      {
        // printf("ProcessRemoteEvictShared\n");
        ProcessRemoteEvictShared(client, wr);
        break;
      }
    case WRITE_BACK:
      {
        // printf("ProcessRemoteEvictDirty\n");
        ProcessRemoteEvictDirty(client, wr);
        break;
      }
#ifdef NOCACHE
    case RLOCK:
      ProcessRemoteRLock(client, wr);
      break;
    case WLOCK:
      ProcessRemoteWLock(client, wr);
      break;
    case RLOCK_REPLY:
    case WLOCK_REPLY:
      ProcessRemoteLockReply(client, wr);
      break;
    case UNLOCK:
      ProcessRemoteUnLock(client, wr);
      break;
#ifndef ASYNC_UNLOCK
    case UNLOCK_REPLY:
      ProcessRemoteUnLockReply(client, wr);
      break;
#endif
#endif
    case REMOTE_THREAD_CREATE:
      {
        ProcessRemoteThreadCreate(client, wr);
        break;
      }
    case REMOTE_THREAD_REPLY:
      {
        ProcessRemoteThreadJoin(client, wr);
        break;
      }
    default:
      epicLog(LOG_WARNING, "unrecognized request from %d",
          client->GetWorkerId());
      exit(-1);
      break;
  }
}

void Worker::ProcessRemoteThreadCreate(Client* client, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "Processing remote thread create request from worker %d", 
          client->GetWorkerId());
  
  // 1. Deserialize thread_id, function name and arguments
  // Buffer layout: [uint64_t thread_id][function_name_string][null_terminator][void* arg]
  char* buffer = (char*)wr->ptr;
  size_t buffer_size = wr->size;
  
  // Extract thread_id (64-bit)
  if (buffer_size < sizeof(uint64_t)) {
    printf("Error: Buffer too small for thread_id\n");
    if (wr->ptr) zfree(wr->ptr);
    delete wr;
    return;
  }
  
  uint64_t thread_id;
  memcpy(&thread_id, buffer, sizeof(uint64_t));
  
  // Extract function name (null-terminated string)
  std::string func_name(buffer + sizeof(uint64_t));
  size_t func_name_len = func_name.length();
  
  // Verify we have enough space for the argument
  size_t expected_size = sizeof(uint64_t) + func_name_len + 1 + sizeof(void*);
  if (buffer_size < expected_size) {
    printf("Error: Invalid buffer size in remote thread request\n");
    if (wr->ptr) zfree(wr->ptr);
    delete wr;
    return;
  }
  
  // Extract arguments from the buffer
  void* args;
  memcpy(&args, buffer + sizeof(uint64_t) + func_name_len + 1, sizeof(void*));
  
  // printf("Received remote thread create: thread_id=%lu, function=%s, args=%p, buffer_size=%zu\n", 
  //        thread_id, func_name.c_str(), args, buffer_size);
  
  // 2. Create RemoteExecutionData for the executor
  RemoteExecutionData* exec_data = new RemoteExecutionData{func_name, args};
  exec_data->thread_id = thread_id;  // Add thread_id to data structure
  
  // 3. Create local pthread to execute remote_thread_executor
  pthread_t local_thread;
  int result = pthread_create(&local_thread, nullptr, 
                             remote_thread_executor, exec_data);
  
  if (result != 0) {
    printf("Error: Failed to create local thread for remote execution: %d\n", result);
    delete exec_data;
    if (wr->ptr) zfree(wr->ptr);
    delete wr;
    return;
  }
  
  // 4. Store mapping for completion notification
  extern void register_remote_thread_mapping(uint64_t thread_id, pthread_t local_thread, Client* source_client);
  register_remote_thread_mapping(thread_id, local_thread, client);
  
  // printf("Created local pthread for remote thread %lu\n", thread_id);
  
  // 5. Cleanup WorkRequest - use zfree for zmalloc'ed memory
  if (wr->ptr) {
    zfree(wr->ptr);  // Use zfree instead of delete[] since ptr was allocated with zmalloc
  }
  delete wr;
}


void Worker::ProcessRemoteThreadJoin(Client* client, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "Processing remote thread completion from worker %d", 
          client->GetWorkerId());
  
  // 1. Extract completion information from buffer
  // Buffer layout: [uint64_t thread_id][void* result]
  char* buffer = (char*)wr->ptr;
  size_t buffer_size = wr->size;
  
  if (buffer_size < sizeof(uint64_t) + sizeof(void*)) {
    printf("Error: Invalid buffer size in remote thread completion\n");
    if (wr->ptr) zfree(wr->ptr);
    delete wr;
    return;
  }
  
  uint64_t thread_id;
  void* result;
  memcpy(&thread_id, buffer, sizeof(uint64_t));
  memcpy(&result, buffer + sizeof(uint64_t), sizeof(void*));
  
  bool error = (wr->status == ERROR);

  epicLog(LOG_DEBUG, "Received thread completion: thread_id=%lu, result=%p, error=%s\n", 
         thread_id, result, error ? "true" : "false");
  
  // 2. Notify the waiting thread through ThreadManager
  extern void handle_remote_thread_completion(uint64_t thread_id, void* result, bool error);
  handle_remote_thread_completion(thread_id, result, error);
  
  // 3. Cleanup WorkRequest
  if (wr->ptr) {
    zfree(wr->ptr);
  }
  delete wr;
}
