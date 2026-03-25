#ifndef _MISCADMIN_H
#define _MISCADMIN_H

extern volatile bool g_gotsigterm;
extern volatile bool g_gotsigusr2;
extern pthread_t     g_mainthrid;
extern char          g_cfgpath[512];
extern char          g_profilepath[512];
extern volatile bool g_closestd;
extern volatile int  g_xsynchstat;
extern volatile int  g_gotsigreload;

extern uint64 g_walrecno;
extern uint64 g_parserecno;

/* program type (capture/integrate/receivewal/xmanager) */
extern int g_proctype;

extern int g_loglevel;

#endif
