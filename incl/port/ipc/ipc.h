#ifndef _RIPPLE_IPC_H
#define _RIPPLE_IPC_H

typedef void (*ripple_on_exit_callback) (int code, void* arg);

pid_t ripple_ipc_fork(void);

int ripple_ipc_pipe(int channel[2]);

#endif
