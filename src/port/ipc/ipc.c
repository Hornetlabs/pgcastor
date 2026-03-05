#include "ripple_app_incl.h"
#include "port/ipc/ipc.h"

#define MAX_ON_EXITS 20

typedef void (*ripple_on_exit_callback) (int code, void* arg);

struct ONEXIT
{
	ripple_on_exit_callback function;
	void*		arg;
};


pid_t ripple_ipc_fork(void)
{
    return fork();
}

/* pipe */
int ripple_ipc_pipe(int pipes[2])
{
	return pipe(pipes);
}
