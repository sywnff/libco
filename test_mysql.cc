// Copyright 2019, Tencent Inc.
// Author: sywnff
// Create: 2019-09-19
// Encoding: utf-8
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <poll.h>
#include <assert.h>
#include <signal.h>
#include <fcntl.h>
#include <list>
#include <string>
#include <mysql/mysql.h>
#include "co_routine.h"

////////////////////////////////////////////////////////////////////////////////


int CoroutineInit();
void CoroutineFini();

static pthread_t g_co_thread;
static bool g_co_require_terminate = false;

struct stCoRoutine_t *g_co[10000];
typedef std::list<MYSQL*> DBPool;

MYSQL* MakeConn();
int MakeDbPool(int size);
void FreeDbPool();

MYSQL* GetDbConn();
void FreeDbConn(MYSQL *conn);

DBPool g_db_pool;
int g_db_pool_size = 0;
int g_db_pool_max_size = 5;
int g_co_num = 10;
unsigned int g_total_num = 0;

const char *g_host = NULL;
const char *g_user = NULL;
const char *g_passwd = NULL;

#define log(fmt, ...) fprintf(stderr, fmt"\n", __VA_ARGS__)

bool g_running = true;

static void HandleSignal(int signo, siginfo_t *, void *) {
  if (signo == SIGINT)
    g_running = false;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "usage: ./test_mysql <db_host> <db_user> <db_passwd> [co_routine_num] [max_db_pool_size]\n");
    return -1;
  }

  g_host = argv[1];
  g_user = argv[2];
  g_passwd = argv[3];
  if (argc > 4) {
    g_co_num = atoi(argv[4]);
  }
  if (argc > 5) {
    g_db_pool_max_size = atoi(argv[5]);
  }

  memset(g_co, 0, sizeof(g_co));

  struct sigaction act;
  memset(&act, 0, sizeof(act));
  act.sa_sigaction = HandleSignal;
  sigaction(SIGINT, &act, NULL);

  MakeDbPool(g_db_pool_max_size);
  CoroutineInit();
  for (;g_running;) {
    usleep(1000000);
  }

  CoroutineFini();
  FreeDbPool();
  return 0;
}


#define ENABLE_CO_ROUTINE

template <typename T>
class ThreadLocalVar {
public:
  ThreadLocalVar() {
#ifdef ENABLE_CO_ROUTINE
    if (pthread_key_create(&key_, nullptr) != 0) {
      int e = errno;
      log("pthread_key_create failed,%d,%s", errno, strerror(e));
      abort();
    }    
#else
    val_ = NULL;
#endif
  }
  
  ~ThreadLocalVar() {
#ifdef ENABLE_CO_ROUTINE
    T* val = reinterpret_cast<T*>(co_getspecific(key_));
    pthread_key_delete(key_);
    key_ = -1;

    if (val != NULL) {
      delete val;
    }    
#else
    T* val = val_;
    val_ = NULL;
    delete val;
#endif
  }

  T* operator ->() {
    return GetVal();
  }

  T& operator &() {
    return *GetVal();
  }

  T* GetVal() {
#ifdef ENABLE_CO_ROUTINE
    T* val = reinterpret_cast<T*>(co_getspecific(key_));  
    if (val == NULL) {
      val = new T();
      co_setspecific(key_, val);      
    }
    return val;
#else
    T* val = val_;
    if (val == NULL) {
      val = new T();
      val_ = val;
    }

    return val;
#endif
  }

  T* GetRaw() {
#ifdef ENABLE_CO_ROUTINE
    return reinterpret_cast<T*>(co_getspecific(key_));    
#else
    return val_;
#endif
  }

  void SetVal(T* val) {
    T* old_val = GetVal();
    if (old_val != NULL) {
      delete old_val;
    }

#ifdef ENABLE_CO_ROUTINE
    co_setspecific(key_, val);
#else
    val_ = val;
#endif
  }

  void SetVal(const T& val) {
    T* valptr = GetVal();
    *valptr = val;
  }

private:
  ThreadLocalVar(const ThreadLocalVar&){}

  // disable, if ThreadLocalVal is static, the T() only call once in
  // ThreadLocalVar::ThreadLocalVar()
  ThreadLocalVar(const T& val){}

#ifdef ENABLE_CO_ROUTINE
  pthread_key_t key_;
#else
  thread_local T* val_;
#endif
};


/*
  create database if not exists db_my_test;
  use db_my_test;
  create table t1(fid int primary key, fname varchar(100), fcreate_time datetime);
 */

std::string GetName(int round) {
  // static thread_local int g_token = 0;
  // static __thread int g_token = 0;
  static ThreadLocalVar<int> g_token;
  if (g_token.GetRaw() == NULL) {
    g_token.SetVal(100);    
  }

  char buf[64] = {0};
  snprintf(buf, sizeof(buf), "%d-%d", round, (&g_token)++);
  return buf;
}

void* Routine(void *key) {
  const char *skey = reinterpret_cast<const char*>(key);
  co_enable_hook_sys();
  log("co run:%p,%s", co_self(), skey);

  int round = 0;
  while (!g_co_require_terminate) {
    MYSQL *conn = GetDbConn();
    assert(conn != NULL);

    char sql[512] = {0};
    int fid = g_total_num++;
    int size = snprintf(
        sql, sizeof(sql),
        "insert ignore into t1(fid,fname,fcreate_time) values(%u,'%s-%s',now())",
        fid, skey, GetName(round).c_str());
    log("%s:%d:begin-insert:%u", skey, round, fid);
    int ret = mysql_real_query(conn, sql, size);
    log("%s:%d:end-insert:%u,ret=%d,%d,%s", skey, round, fid,
        ret, mysql_errno(conn), mysql_error(conn));

    if (ret != 0) {
      FreeDbConn(conn);
      poll(NULL, 0, 10);
      continue;
    }

    size = snprintf(
        sql, sizeof(sql),
        "select fcreate_time from t1 where fid=%u",
        fid);
    log("%s:%d:begin-query:%u", skey, round, fid);
    ret = mysql_real_query(conn, sql, size);
    assert(ret == 0);

    log("%s:%d:begin-store:%u", skey, round, fid);
    MYSQL_RES* res = mysql_store_result(conn);
    assert(res != NULL);

    log("%s:%d:end-fetch:%u", skey, round, fid);
    MYSQL_ROW row = mysql_fetch_row(res);
    assert(row != NULL);

    mysql_free_result(res);
    FreeDbConn(conn);

    round++;
  }

  return NULL;
}

void CreateRoutine(int num) {
  char *name = NULL;
  for (int i = 0; i < num; ++i) {
    name = new char[16];
    sprintf(name, "r-%d", i);
    co_create(&g_co[i], NULL, Routine, name);
    assert(g_co[i] != NULL);
    co_resume(g_co[i]);
  }
}


// Called each loop round
static int CoTailProc(void *) {
  // log("%s", "epoll round");
  if (g_co_require_terminate) {
    // return -1 to shutdown co_eventloop
    return -1;
  }

  return 0;
}

static void *CoMain(void *) {
  g_co_thread = pthread_self();
  CreateRoutine(g_co_num);
  fprintf(stderr, "coroutine thread started, %d coroutines\n",
          g_co_num);

  co_eventloop(co_get_epoll_ct(), CoTailProc, NULL);

  for (int i = 0; i < g_co_num; ++i) {
    if (g_co[i] != NULL)
      co_release(g_co[i]);
    g_co[i] = NULL;
  }

  fprintf(stderr, "coroutine thread exited.\n");
  return NULL;
}

int CoroutineInit() {
  assert(g_co_thread == 0);
  const int kCoThreadStackSize = 256 << 20; // 256M

  // maybe create a coroutine thread pool,
  // threads in pool share the CoRoutine objects equally (g_co_routines)
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, kCoThreadStackSize);

  pthread_create(&g_co_thread, &attr, CoMain, NULL);
  assert(g_co_thread != 0);
  return 0;
}

void CoroutineFini() {
	if (g_co_thread == 0)
		return;

	g_co_require_terminate = true;
	pthread_join(g_co_thread, NULL);

	g_co_thread = 0;
}

int SetFdBlocking(int fd, bool is_block) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    log("fctnl(F_GETFL) failed:fd=%d", fd);
    return -1;
  }

  if (is_block) {
    flags &= ~O_NONBLOCK;
  } else {
    flags |= O_NONBLOCK;
  }

  if (fcntl(fd, F_SETFL, flags) != 0) {
    log("fctnl(F_SETFL) failed:fd=%d,flags=%d", fd, flags);
    return -1;
  }

  return 0;
}

void EnableCoAsyncIo(int fd) {
  SetFdBlocking(fd, false);
  rpchook_t *hk = alloc_by_fd(fd);
  int domain = 0;
  socklen_t opt_size = sizeof(domain);
  getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &domain, &opt_size);
  hk->domain = domain;
}

MYSQL* MakeConn() {
  MYSQL *conn = mysql_init(NULL);
  assert(conn != NULL);

  MYSQL* ret = mysql_real_connect(
      conn, g_host, g_user, g_passwd,
      "db_my_test", 0, NULL,
      0);
  if (ret == NULL) {
    mysql_close(conn);
    return NULL;
  }

  // My MYSQL version: 5.5.50, relative to mysql api version
  int fd = conn->net.fd;
  log("set db sock noblocking:%d", fd);
  EnableCoAsyncIo(fd);

  return conn;
}

int MakeDbPool(int size) {
  for (int i = 0; i < size; ++i) {
    MYSQL *conn = MakeConn();
    assert(conn != NULL);
    g_db_pool.push_back(conn);
    log("create conn:%d", i);
  }

  log("db pool size:%zd", g_db_pool.size());
  g_db_pool_size = g_db_pool.size();
  return 0;
}

void FreeDbPool() {
  for (auto conn : g_db_pool) {
    mysql_close(conn);
  }

  if (g_db_pool.size() != g_db_pool_size) {
    log("warning: %d conn not give back", g_db_pool_size - g_db_pool.size());
  }

  g_db_pool.clear();
}

MYSQL* GetDbConn() {
  int wait_ms = 0;
  while (true) {
    if (!g_db_pool.empty()) {
        MYSQL* conn = g_db_pool.front();
        g_db_pool.pop_front();
        return conn;
    }

    if (g_db_pool_size < g_db_pool_max_size) {
      MYSQL *conn = MakeConn();
      assert(conn != NULL);
      g_db_pool_size++;
      return conn;
    }

    poll(NULL, 0, wait_ms++ % 10);
  }

  return NULL;
}

void FreeDbConn(MYSQL *conn) {
  g_db_pool.push_back(conn);
}
