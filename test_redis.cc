#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "co_routine.h"
#include "hiredis.h"

int CoroutineInit();
void CoroutineFini();

static pthread_t g_co_thread;
static bool g_co_require_terminate = false;

struct stCoRoutine_t *g_co[1000];
int main() {
	memset(g_co, 0, sizeof(g_co));
	CoroutineInit();
	for (;;) {
		usleep(1000000);
	}

	CoroutineFini();
	return 0;
}

void* Routine(void *key) {
	const char *skey = reinterpret_cast<const char*>(key);
	co_enable_hook_sys();
	printf("co run:%p,%s\n", co_self(), skey);
	redisContext *conn = redisConnect("127.0.0.1", 7777);
	if (conn == NULL) {
		printf("connect redis failed.\n");
		return NULL;
	}

	int round = 0;

	while (!g_co_require_terminate) {
		redisReply *reply = reinterpret_cast<redisReply*>(redisCommand(conn, "set %s %d", skey, round));
		freeReplyObject(reply);

		poll(NULL, 0, random() % 1000);

		reply = reinterpret_cast<redisReply*>(redisCommand(conn, "get %s", skey));

		printf("round:%d %s=%s\n", round, skey, reply->str);
		freeReplyObject(reply);
		round++;
	}

	redisFree(conn);
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
	assert(g_co_thread == NULL);
	const int kCoThreadStackSize = 256 << 20; // 256M

	// maybe create a coroutine thread pool,
	// threads in pool share the CoRoutine objects equally (g_co_routines)	
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, kCoThreadStackSize);

	pthread_create(&g_co_thread, &attr, CoMain, NULL);
	assert(g_co_thread != NULL);
	return 0;
}

void CoroutineFini() {
	if (g_co_thread == NULL)
		return;

	g_co_require_terminate = true;
	pthread_join(g_co_thread, NULL);

	g_co_thread = NULL;	
}

