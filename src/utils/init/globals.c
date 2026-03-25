#include "app_incl.h"

volatile bool g_gotsigterm = false;
volatile bool g_gotsigusr2 = false;
volatile bool g_closestd = false;
volatile int  g_xsynchstat = XSYNCHSTAT_INIT;
volatile int  g_gotsigreload = CAPTURERELOAD_STATUS_UNSET;

char g_profilepath[MAXPATH] = {0};

pthread_t g_mainthrid = 0;

uint64 g_walrecno = 0;
uint64 g_parserecno = 0;

int g_proctype = PROC_TYPE_NOP;
