#ifndef _IPC_H
#define _IPC_H

typedef void (*on_exit_callback)(int code, void* arg);

pid_t osal_ipc_fork(void);

int osal_ipc_pipe(int channel[2]);

#endif
