#ifndef _RIPPLE_REFRESH_PUMPSHARDINGNET_H
#define _RIPPLE_REFRESH_PUMPSHARDINGNET_H

typedef struct RIPPLE_REFRESH_PUMPSHARDINGNETSTATE
{
    ripple_netclient        base;
    rsocket                 state;                      /* 工作状态 */
    int                     fd;                 /* 当前被发送文件描述符 */
    uint64                  filesize;           /* 当前被发送文件的大小 */
    uint64                  fileoffset;
    char                    refeshpath[RIPPLE_MAXPATH];
    char                    filepath[RIPPLE_ABSPATH];
}ripple_refresh_pumpshardingnetstate;

extern ripple_refresh_pumpshardingnetstate* ripple_refresh_pumpshardingnet_init(void);

extern void ripple_refresh_pumpshardingnet_destroy(ripple_refresh_pumpshardingnetstate* clientstate);

#endif