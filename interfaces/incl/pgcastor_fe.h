#ifndef _PGCASTOR_FE_H_
#define _PGCASTOR_FE_H_

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

typedef enum PGCASTOR_CMDTAG
{
    T_PGCASTOR_NOP = 0,
    T_PGCASTOR_IDENTITYCMD,
    T_PGCASTOR_CREATECMD,
    T_PGCASTOR_ALTERCMD,
    T_PGCASTOR_REMOVECMD,
    T_PGCASTOR_DROPCMD,
    T_PGCASTOR_INITCMD,
    T_PGCASTOR_EDITCMD,
    T_PGCASTOR_STARTCMD,
    T_PGCASTOR_STOPCMD,
    T_PGCASTOR_RELOADCMD,
    T_PGCASTOR_INFOCMD,
    T_PGCASTOR_WATCHCMD,
    T_PGCASTOR_CFGfILECMD,
    T_PGCASTOR_REFRESHCMD,
    T_PGCASTOR_LISTCMD,

    /* add before this */
    T_PGCASTOR_MAX
} pgcastor_cmdtag;

typedef struct PGCASTOR_CMD
{
    pgcastor_cmdtag type;
} pgcastor_cmd;

typedef enum PGCASTOR_JOBKIND
{
    PGCASTOR_JOBKIND_NOP = 0,
    PGCASTOR_JOBKIND_CAPTURE,
    PGCASTOR_JOBKIND_INTEGRATE,
    PGCASTOR_JOBKIND_PGRECEIVELOG,
    PGCASTOR_JOBKIND_PROCESS,

    /*---------add specific business types above ------*/

    /* applies to start/stop/info/watch */
    PGCASTOR_JOBKIND_ALL,

    /* xmanager */
    PGCASTOR_JOBKIND_MANAGER,

    /* XSCSCI */
    PGCASTOR_JOBKIND_XSCSCI,

    /* add before this */
    PGCASTOR_JOBKIND_MAX
} pgcastor_jobkind;

typedef enum PGCASTOR_ACTION
{
    PGCASTOR_ACTION_NOP = 0,
    PGCASTOR_ACTION_ADD,
    PGCASTOR_ACTION_REMOVE,

    /* add before this */
    PGCASTOR_ACTION_MAX
} pgcastor_action;

typedef struct PGCASTOR_RANGEVAR
{
    char* schema;
    char* table;
} pgcastor_rangevar;

typedef struct PGCASTOR_JOB
{
    pgcastor_jobkind kind;
    char*            jobname;
} pgcastor_job;

/* create specified job type */
typedef struct PGCASTOR_IDENTITYCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /*
     * no current use
     */
    /* username */
    char*            user;

    /* password */
    char*            passwd;

    /* job name */
    char*            jobname;
} pgcastor_identitycmd;

typedef struct PGCASTOR_CREATECMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;

    /* pgcastor_job */
    List*            job;
} pgcastor_createcmd;

/* modify process job members */
typedef struct PGCASTOR_ALTERCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* operation type, add/remove */
    pgcastor_action  action;

    /* name */
    char*            name;

    /* pgcastor_job */
    List*            job;
} pgcastor_altercmd;

/* delete config file for specified job type */
typedef struct PGCASTOR_REMOVECMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_removecmd;

/* drop specified job */
typedef struct PGCASTOR_DROPCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_dropcmd;

/* initialize specified job */
typedef struct PGCASTOR_INITCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_initcmd;

/* edit specified job config file */
typedef struct PGCASTOR_EDITCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_editcmd;

/* start specified job */
typedef struct PGCASTOR_STARTCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_startcmd;

/* stop specified job */
typedef struct PGCASTOR_STOPCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_stopcmd;

/* reload config file for specified job */
typedef struct PGCASTOR_RELOADCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_reloadcmd;

/* view basic info of specified job */
typedef struct PGCASTOR_INFOCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;
} pgcastor_infocmd;

/* view basic info of specified job */
typedef struct PGCASTOR_LISTCMD
{
    pgcastor_cmd type;
} pgcastor_listcmd;

/* periodically return info of specified job */
typedef struct PGCASTOR_WATCHCMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* interval, unit: seconds */
    int              interval;

    /* job name */
    char*            name;
} pgcastor_watchcmd;

/* send config file information */
typedef struct PGCASTOR_CFGFILECMD
{
    pgcastor_cmd     type;

    /* job type */
    pgcastor_jobkind kind;

    /* job name */
    char*            name;

    /* file name */
    char*            filename;

    /* file content */
    int              datalen;

    /* file content */
    char*            data;
} pgcastor_cfgfilecmd;

/* refresh command */
typedef struct PGCASTOR_REFRESHCMD
{
    pgcastor_cmd type;

    /* capture name */
    char*        name;

    /* pgcastor_rangevar */
    List*        tables;
} pgcastor_refreshcmd;

#define PGCASTOR_NEWCMD(size, tag)             \
    ({                                         \
        pgcastor_cmd* _result = NULL;          \
        _result = (pgcastor_cmd*)malloc(size); \
        _result->type = (tag);                 \
        _result;                               \
    })

#define PGCASTOR_MAKECMD(_type_) ((struct _type_*)PGCASTOR_NEWCMD(sizeof(struct _type_), T_##_type_))

/* key/value pair */
typedef struct PGCASTORPAIR
{
    int   keylen;
    int   valuelen;
    char* key;

    /* value may be empty, when value is empty, it means value not found or no such column info */
    char* value;
} pgcastorpair;

/* row values */
typedef struct PGCASTORROW
{
    int           columncnt;
    pgcastorpair* columns;
} pgcastorrow;

/* connection status */
typedef enum PGCASTORCONN_STATUS
{
    PGCASTORCONN_STATUS_NOP = 0x00,

    /* connecting */
    PGCASTORCONN_STATUS_INPROCESS,

    /* connected */
    PGCASTORCONN_STATUS_OK
} pgcastorconn_status;

typedef struct PGCASTOR_CONN pgcastorconn;

/* initialize pgcastor_result */
void* PGCastorResultInit(void);

/* reset pgcastor_result content */
void PGCastorResultReset(void* in_result);

/* free pair resources */
pgcastorrow* PGCastorRowInit(int rowcnt);

/* initialize pgcastorpair */
pgcastorpair* PGCastorPairInit(int colcnt);

/* free rows resources */
void PGCastorRowFree(int rowcnt, void* in_rows);

/* free pair resources */
void PGCastorPairFree(int colcnt, void* in_col);

/* set connection parameters */
pgcastorconn* PGCastorSetParam(char* connstr);

/* connect to xmanager */
bool PGCastorConn(pgcastorconn* conn);

/* test if xmanager is started */
bool PGCastorPing(pgcastorconn* conn);

/* send command */
bool PGCastorSendCmd(pgcastorconn* conn, pgcastor_cmd* cmd);

/* get return result */
void PGCastorGetResult(pgcastorconn* conn, int* rownumber, pgcastorrow** rows);

/* clean return result */
void PGCastorClear(pgcastorconn* conn);

void PGCastorGetErrmsg(pgcastorconn* conn);

/* clean pgcastorconn */
void PGCastorFinish(pgcastorconn* conn);

void pgcastor_rangvar_destroy(pgcastor_rangevar* rs);

pgcastor_rangevar* pgcastor_rangvar_init(char* schema, char* table);

void pgcastor_rangvar_free(pgcastor_rangevar* rangevar);

pgcastor_job* pgcastor_job_init(int jobkind, char* jobname);

void pgcastor_job_free(pgcastor_job* job);

void pgcastor_command_free(pgcastor_cmd* cmd);

#endif
