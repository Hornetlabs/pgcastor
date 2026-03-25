#include "app_incl.h"
#include "port/ipc/ipc.h"

#define MAX_ON_EXITS 20

typedef void (*on_exit_callback)(int code, void* arg);

struct ONEXIT
{
    on_exit_callback function;
    void*            arg;
};

pid_t osal_ipc_fork(void)
{
    return fork();
}

/* pipe */
int osal_ipc_pipe(int pipes[2])
{
    return pipe(pipes);
}
