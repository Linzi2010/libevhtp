#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>
#include <sys/queue.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/queue.h>

#include "evthr.h"

#if (__GNUC__ > 2 || ( __GNUC__ == 2 && __GNUC__MINOR__ > 4)) && (!defined(__STRICT_ANSI__) || __STRICT_ANSI__ == 0)
#define __unused__   __attribute__((unused))
#else
#define __unused__
#endif

#define _EVTHR_MAGIC 0x03fb

typedef struct evthr_cmd        evthr_cmd_t;
typedef struct evthr_pool_slist evthr_pool_slist_t;
typedef struct evthr_queue      evthr_queue_t;
typedef struct evthr_work_item  evthr_work_item_t;

struct evthr_cmd {
    STAILQ_ENTRY(evthr_cmd) next;
    void   * args;
    evthr_cb cb;
    uint8_t  stop : 1;
};

TAILQ_HEAD(evthr_pool_slist, evthr);

STAILQ_HEAD(evthr_queue, evthr_cmd);

struct evthr_pool {
    int                nthreads;
    evthr_pool_slist_t threads;
};

struct evthr {
    int             cur_backlog;
    int             max_backlog;
    char            err;

    evthr_queue_t   queue;

    pthread_cond_t  cond; /* protected by rlock */

    pthread_mutex_t lock;
    pthread_mutex_t stat_lock;
    pthread_mutex_t rlock;

    pthread_t     * thr;
    evthr_init_cb   init_cb;
    void          * arg;
    void          * aux;

    TAILQ_ENTRY(evthr) next;
};

inline void
evthr_inc_backlog(evthr_t * evthr) {
    __sync_fetch_and_add(&evthr->cur_backlog, 1);
}

inline void
evthr_dec_backlog(evthr_t * evthr) {
    __sync_fetch_and_sub(&evthr->cur_backlog, 1);
}

inline int
evthr_get_backlog(evthr_t * evthr) {
    return __sync_add_and_fetch(&evthr->cur_backlog, 0);
}

inline void
evthr_set_max_backlog(evthr_t * evthr, int max) {
    evthr->max_backlog = max;
}

static void *
_evthr_loop(void * args) {
    evthr_t * thread;
    evthr_cmd_t *cmd;

    if (!(thread = (evthr_t *)args)) {
        return NULL;
    }

    if (thread == NULL || thread->thr == NULL) {
        pthread_exit(NULL);
    }

    pthread_mutex_lock(&thread->lock);
    if (thread->init_cb != NULL) {
        thread->init_cb(thread, thread->arg);
    }

    pthread_mutex_lock(&thread->rlock);
    while (pthread_cond_wait(&thread->cond, &thread->rlock) == 0) {
        while ((cmd = STAILQ_FIRST(&thread->queue))) {

            STAILQ_REMOVE_HEAD(&thread->queue, next);
            if (cmd->stop == 1)
                break;
            pthread_mutex_unlock(&thread->rlock);

            if (cmd->cb != NULL) {
                cmd->cb(thread, cmd->args, thread->arg);
            }
            evthr_dec_backlog(thread);

            pthread_mutex_lock(&thread->rlock);
        }
    }

    pthread_mutex_unlock(&thread->rlock);
    pthread_mutex_unlock(&thread->lock);

    if (thread->err == 1) {
        fprintf(stderr, "FATAL ERROR!\n");
    }

    evthr_free(thread);
    pthread_exit(NULL);
}

evthr_res
evthr_defer(evthr_t * thread, evthr_cb cb, void * arg) {
    int         cur_backlog;
    evthr_cmd_t *cmd;

    cur_backlog = evthr_get_backlog(thread);

    if (thread->max_backlog) {
        if (cur_backlog + 1 > thread->max_backlog) {
            return EVTHR_RES_BACKLOG;
        }
    }

    if (cur_backlog == -1) {
        return EVTHR_RES_FATAL;
    }

    cmd = malloc(sizeof(*cmd));
    /* cmd.magic = _EVTHR_MAGIC; */
    cmd->cb   = cb;
    cmd->args = arg;
    cmd->stop = 0;

    pthread_mutex_lock(&thread->rlock);

    evthr_inc_backlog(thread);

    STAILQ_INSERT_TAIL(&thread->queue, cmd, next);

    pthread_cond_signal(&thread->cond);

    pthread_mutex_unlock(&thread->rlock);

    return EVTHR_RES_OK;
}

evthr_res
evthr_stop(evthr_t * thread) {
    evthr_cmd_t *cmd = malloc(sizeof(*cmd));

    /* cmd.magic = _EVTHR_MAGIC; */
    cmd->cb   = NULL;
    cmd->args = NULL;
    cmd->stop = 1;

    pthread_mutex_lock(&thread->rlock);

    STAILQ_INSERT_TAIL(&thread->queue, cmd, next);

    pthread_cond_signal(&thread->cond);

    pthread_mutex_unlock(&thread->rlock);

    return EVTHR_RES_OK;
}

void
evthr_set_aux(evthr_t * thr, void * aux) {
    thr->aux = aux;
}

void *
evthr_get_aux(evthr_t * thr) {
    return thr->aux;
}

evthr_t *
evthr_new(evthr_init_cb init_cb, void * args) {
    evthr_t * thread;

    if (!(thread = calloc(sizeof(evthr_t), 1))) {
        return NULL;
    }

    thread->thr     = malloc(sizeof(pthread_t));
    thread->init_cb = init_cb;
    thread->arg     = args;
    STAILQ_INIT(&thread->queue);

    if (pthread_mutex_init(&thread->lock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    if (pthread_mutex_init(&thread->stat_lock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    if (pthread_mutex_init(&thread->rlock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    if (pthread_cond_init(&thread->cond, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    return thread;
} /* evthr_new */

int
evthr_start(evthr_t * thread) {
    int res;

    if (thread == NULL || thread->thr == NULL) {
        return -1;
    }

    if (pthread_create(thread->thr, NULL, _evthr_loop, (void *)thread)) {
        return -1;
    }

    res = pthread_detach(*thread->thr);

    return res;
}

void
evthr_free(evthr_t * thread) {
    if (thread == NULL) {
        return;
    }

    if (thread->thr) {
        free(thread->thr);
    }

    /* XXX what about pthread_mutex_destroy?? */

    pthread_cond_destroy(&thread->cond);

    free(thread);
} /* evthr_free */

void
evthr_pool_free(evthr_pool_t * pool) {
    evthr_t * thread;
    evthr_t * save;

    if (pool == NULL) {
        return;
    }

    for (thread = TAILQ_FIRST(&pool->threads); thread != NULL; thread = save) {
        save = TAILQ_NEXT(thread, next);

        TAILQ_REMOVE(&pool->threads, thread, next);

        evthr_free(thread);
    }

    free(pool);
}

evthr_res
evthr_pool_stop(evthr_pool_t * pool) {
    evthr_t * thr;

    if (pool == NULL) {
        return EVTHR_RES_FATAL;
    }

    TAILQ_FOREACH(thr, &pool->threads, next) {
        evthr_stop(thr);
    }

    memset(&pool->threads, 0, sizeof(pool->threads));

    return EVTHR_RES_OK;
}

evthr_res
evthr_pool_defer(evthr_pool_t * pool, evthr_cb cb, void * arg) {
    evthr_t * min_thr = NULL;
    evthr_t * thr     = NULL;

    if (pool == NULL) {
        return EVTHR_RES_FATAL;
    }

    if (cb == NULL) {
        return EVTHR_RES_NOCB;
    }

    /* find the thread with the smallest backlog */
    TAILQ_FOREACH(thr, &pool->threads, next) {
        evthr_t * m_save;
        evthr_t * t_save;
        int       thr_backlog = 0;
        int       min_backlog = 0;

        thr_backlog = evthr_get_backlog(thr);

        if (min_thr) {
            min_backlog = evthr_get_backlog(min_thr);
        }

        m_save = min_thr;
        t_save = thr;

        if (min_thr == NULL) {
            min_thr = thr;
        } else if (thr_backlog == 0) {
            min_thr = thr;
        } else if (thr_backlog < min_backlog) {
            min_thr = thr;
        }

        if (evthr_get_backlog(min_thr) == 0) {
            break;
        }
    }

    return evthr_defer(min_thr, cb, arg);
} /* evthr_pool_defer */

evthr_pool_t *
evthr_pool_new(int nthreads, evthr_init_cb init_cb, void * shared) {
    evthr_pool_t * pool;
    int            i;

    if (nthreads == 0) {
        return NULL;
    }

    if (!(pool = calloc(sizeof(evthr_pool_t), sizeof(char)))) {
        return NULL;
    }

    pool->nthreads = nthreads;
    TAILQ_INIT(&pool->threads);

    for (i = 0; i < nthreads; i++) {
        evthr_t * thread;

        if (!(thread = evthr_new(init_cb, shared))) {
            evthr_pool_free(pool);
            return NULL;
        }

        TAILQ_INSERT_TAIL(&pool->threads, thread, next);
    }

    return pool;
}

void
evthr_pool_set_max_backlog(evthr_pool_t * pool, int max) {
    evthr_t * thr;

    TAILQ_FOREACH(thr, &pool->threads, next) {
        evthr_set_max_backlog(thr, max);
    }
}

int
evthr_pool_start(evthr_pool_t * pool) {
    evthr_t * evthr = NULL;

    if (pool == NULL) {
        return -1;
    }

    TAILQ_FOREACH(evthr, &pool->threads, next) {
        if (evthr_start(evthr) < 0) {
            return -1;
        }

        usleep(5000);
    }

    return 0;
}

