#ifndef _XSYNCH_FE_H_
#define _XSYNCH_FE_H_

#ifndef bool
typedef int8_t bool;
#endif

#ifndef true
#define true ((bool)1)
#endif

#ifndef false
#define false ((bool)0)
#endif

typedef unsigned int Oid;

#include "list_func.h"

typedef enum XSYNCH_CMDTAG
{
    T_XSYNCH_NOP = 0,
    T_XSYNCH_IDENTITYCMD,
    T_XSYNCH_CREATECMD,
    T_XSYNCH_ALTERCMD,
    T_XSYNCH_REMOVECMD,
    T_XSYNCH_DROPCMD,
    T_XSYNCH_INITCMD,
    T_XSYNCH_EDITCMD,
    T_XSYNCH_STARTCMD,
    T_XSYNCH_STOPCMD,
    T_XSYNCH_RELOADCMD,
    T_XSYNCH_INFOCMD,
    T_XSYNCH_WATCHCMD,
    T_XSYNCH_CFGfILECMD,
    T_XSYNCH_REFRESHCMD,
    T_XSYNCH_LISTCMD,

    /* add before this */
    T_XSYNCH_MAX
} xsynch_cmdtag;

typedef struct XSYNCH_CMD
{
    xsynch_cmdtag type;
} xsynch_cmd;

typedef enum XSYNCH_JOBKIND
{
    XSYNCH_JOBKIND_NOP = 0,
    XSYNCH_JOBKIND_CAPTURE,
    XSYNCH_JOBKIND_INTEGRATE,
    XSYNCH_JOBKIND_PGRECEIVELOG,
    XSYNCH_JOBKIND_PROCESS,

    /*---------add specific business types above ------*/

    /* applies to start/stop/info/watch */
    XSYNCH_JOBKIND_ALL,

    /* xmanager */
    XSYNCH_JOBKIND_MANAGER,

    /* XSCSCI */
    XSYNCH_JOBKIND_XSCSCI,

    /* add before this */
    XSYNCH_JOBKIND_MAX
} xsynch_jobkind;

typedef enum XSYNCH_ACTION
{
    XSYNCH_ACTION_NOP = 0,
    XSYNCH_ACTION_ADD,
    XSYNCH_ACTION_REMOVE,

    /* add before this */
    XSYNCH_ACTION_MAX
} xsynch_action;

typedef struct XSYNCH_RANGEVAR
{
    char* schema;
    char* table;
} xsynch_rangevar;

typedef struct XSYNCH_JOB
{
    xsynch_jobkind kind;
    char*          jobname;
} xsynch_job;

/* create specified job type */
typedef struct XSYNCH_IDENTITYCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /*
     * no current use
     */
    /* username */
    char*          user;

    /* password */
    char*          passwd;

    /* job name */
    char*          jobname;
} xsynch_identitycmd;

typedef struct XSYNCH_CREATECMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;

    /* xsynch_job */
    List*          job;
} xsynch_createcmd;

/* modify process job members */
typedef struct XSYNCH_ALTERCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* operation type, add/remove */
    xsynch_action  action;

    /* name */
    char*          name;

    /* xsynch_job */
    List*          job;
} xsynch_altercmd;

/* delete config file for specified job type */
typedef struct XSYNCH_REMOVECMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_removecmd;

/* drop specified job */
typedef struct XSYNCH_DROPCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_dropcmd;

/* initialize specified job */
typedef struct XSYNCH_INITCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_initcmd;

/* edit specified job config file */
typedef struct XSYNCH_EDITCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_editcmd;

/* start specified job */
typedef struct XSYNCH_STARTCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_startcmd;

/* stop specified job */
typedef struct XSYNCH_STOPCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_stopcmd;

/* reload config file for specified job */
typedef struct XSYNCH_RELOADCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_reloadcmd;

/* view basic info of specified job */
typedef struct XSYNCH_INFOCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;
} xsynch_infocmd;

/* view basic info of specified job */
typedef struct XSYNCH_LISTCMD
{
    xsynch_cmd type;
} xsynch_listcmd;

/* periodically return info of specified job */
typedef struct XSYNCH_WATCHCMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* interval, unit: seconds */
    int            interval;

    /* job name */
    char*          name;
} xsynch_watchcmd;

/* send config file information */
typedef struct XSYNCH_CFGFILECMD
{
    xsynch_cmd     type;

    /* job type */
    xsynch_jobkind kind;

    /* job name */
    char*          name;

    /* file name */
    char*          filename;

    /* file content */
    int            datalen;

    /* file content */
    char*          data;
} xsynch_cfgfilecmd;

/* refresh command */
typedef struct XSYNCH_REFRESHCMD
{
    xsynch_cmd type;

    /* capture name */
    char*      name;

    /* xsynch_rangevar */
    List*      tables;
} xsynch_refreshcmd;

#define XSYNCH_NEWCMD(size, tag)             \
    ({                                       \
        xsynch_cmd* _result = NULL;          \
        _result = (xsynch_cmd*)malloc(size); \
        _result->type = (tag);               \
        _result;                             \
    })

#define XSYNCH_MAKECMD(_type_) ((struct _type_*)XSYNCH_NEWCMD(sizeof(struct _type_), T_##_type_))

/* key/value pair */
typedef struct XSYNCHPAIR
{
    int   keylen;
    int   valuelen;
    char* key;

    /* value may be empty, when value is empty, it means value not found or no such column info */
    char* value;
} xsynchpair;

/* row values */
typedef struct XSYNCHROW
{
    int         columncnt;
    xsynchpair* columns;
} xsynchrow;

/* connection status */
typedef enum XSYNCHCONN_STATUS
{
    XSYNCHCONN_STATUS_NOP = 0x00,

    /* connecting */
    XSYNCHCONN_STATUS_INPROCESS,

    /* connected */
    XSYNCHCONN_STATUS_OK
} xsynchconn_status;

typedef struct XSYNCH_CONN xsynchconn;

/* initialize xsynch_result */
void* XsynchResultInit(void);

/* reset xsynch_result content */
void XsynchResultReset(void* in_result);

/* free pair resources */
xsynchrow* XsynchRowInit(int rowcnt);

/* initialize xsynchpair */
xsynchpair* XsynchPairInit(int colcnt);

/* free rows resources */
void XsynchRowFree(int rowcnt, void* in_rows);

/* free pair resources */
void XsynchPairFree(int colcnt, void* in_col);

/* set connection parameters */
xsynchconn* XSynchSetParam(char* connstr);

/* connect to xmanager */
bool XSynchConn(xsynchconn* conn);

/* test if xmanager is started */
bool XSynchPing(xsynchconn* conn);

/* send command */
bool XSynchSendCmd(xsynchconn* conn, xsynch_cmd* cmd);

/* get return result */
void XSynchGetResult(xsynchconn* conn, int* rownumber, xsynchrow** rows);

/* clean return result */
void XSynchClear(xsynchconn* conn);

void XSynchGetErrmsg(xsynchconn* conn);

/* clean xsynchconn */
void XSynchFinish(xsynchconn* conn);

void xsynch_rangvar_destroy(xsynch_rangevar* rs);

xsynch_rangevar* xsynch_rangvar_init(char* schema, char* table);

void xsynch_rangvar_free(xsynch_rangevar* rangevar);

xsynch_job* xsynch_job_init(int jobkind, char* jobname);

void xsynch_job_free(xsynch_job* job);

void xsynch_command_free(xsynch_cmd* cmd);

#endif
