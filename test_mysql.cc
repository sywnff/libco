// Copyright 2019, Tencent Inc.
// Author: bondshi<bondshi@tencent.com>
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
#include <mysql/mysql.h>
#include "co_routine.h"

////////////////////////////////////////////////////////////////////////////////

int CoroutineInit();
void CoroutineFini();

static pthread_t g_co_thread;
static bool g_co_require_terminate = false;

struct stCoRoutine_t *g_co[1000];
typedef std::list<MYSQL*> DBPool;

MYSQL* MakeConn();
int MakeDbPool(int size);
void FreeDbPool();

MYSQL* GetDbConn();
void FreeDbConn(MYSQL *conn);

DBPool g_db_pool;
int g_db_pool_size = 0;
int g_db_pool_max_size = 5;
unsigned int total_num = 0;

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
    fprintf(stderr, "usage: ./test_mysql <db_host> <db_user> <db_passwd> [max_db_pool_size]\n");
    return -1;
  }

  g_host = argv[1];
  g_user = argv[2];
  g_passwd = argv[3];
  if (argc > 4) {
    g_db_pool_max_size = atoi(argv[4]);
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

/*
  create database if not exists db_my_test;
  use db_my_test;
  create table t1(fid int primary key, fname varchar(100), fcreate_time datetime);
 */

void* Routine(void *key) {
	const char *skey = reinterpret_cast<const char*>(key);
	co_enable_hook_sys();
	log("co run:%p,%s", co_self(), skey);

	int round = 0;
	while (!g_co_require_terminate) {
      MYSQL *conn = GetDbConn();
      assert(conn != NULL);

      char sql[512] = {0};
      int fid = total_num++;
      int size = snprintf(
      sql, sizeof(sql),
          "insert ignore into t1(fid,fname,fcreate_time) values(%u,'%s-%d',now())",
          fid, skey, round);
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


// called each loop round
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
	int num = 100;

	CreateRoutine(num);
	fprintf(stderr, "coroutine thread started, %d coroutines\n",
		1);

	co_eventloop(co_get_epoll_ct(), CoTailProc, NULL);

	for (int i = 0; i < num; ++i) {
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

static inline int GetMySqlFd(const MYSQL *conn) {
  // maybe different on mysql client api version
  // mysql client version: 5.5.50
  return conn->net.fd;
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

  int fd = GetMySqlFd(conn);
  log("set db sock noblocking:%d", fd);
  SetFdBlocking(fd, false);

  rpchook_t *hk = alloc_by_fd(fd);
  hk->domain = (conn->unix_socket==NULL)?AF_INET:AF_LOCAL;

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
