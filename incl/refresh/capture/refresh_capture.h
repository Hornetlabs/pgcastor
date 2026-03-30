#ifndef _REFRESH_CAPTURE_H
#define _REFRESH_CAPTURE_H

/* Refresh thread controller structure */
typedef struct REFRESH_CAPTURE
{
    int             parallelcnt;
    char*           conn_info;      /* Save connection string */
    char*           snap_shot_name; /* Save snapshot name */
    char*           refresh_path;   /* Refresh folder path */
    thrsubmgr*      thrsmgr;
    PGconn*         conn;   /* libpq connection handle */
    refresh_tables* tables; /* Tables to refresh information */
    queue*          tqueue; /* Task queue */
} refresh_capture;

extern refresh_capture* refresh_capture_init(void);

/* Set snapshot name */
extern void refresh_capture_setsnapshotname(refresh_capture* rcapture, char* snapname);

extern void refresh_capture_setrefreshtables(refresh_tables* tables, refresh_capture* mgr);

extern void refresh_capture_setconn(PGconn* conn, refresh_capture* mgr);

extern void* refresh_capture_main(void* args);

extern void refresh_capture_free(void* privdata);

#endif
