#ifndef _TASK_SLOT_H
#define _TASK_SLOT_H

typedef enum TASK_TYPE
{
    TASKTYPE_NOP = 0x00,
    TASKTYPE_REFRESHSHARDING                         ,
    TASKTYPE_REFRESHP2CSHARDING                      ,
    TASKTYPE_REFRESHSHARDING2DB                      ,
    TASKTYPE_ONLINEREFRESH_CAPTURE_PARSERWAL         ,
    TASKTYPE_ONLINEREFRESH_CAPTURE_SPLITWAL          ,
    TASKTYPE_ONLINEREFRESH_CAPTURE_SERIAL            ,
    TASKTYPE_ONLINEREFRESH_CAPTURE_WRITE             ,
    TASK_TYPE_INTEGRATE_ONLINEREFRESH_SPLITTRAIL     ,
    TASK_TYPE_INTEGRATE_ONLINEREFRESH_PARSERTRAIL    ,
    TASK_TYPE_INTEGRATE_ONLINEREFRESH_REBUILD        ,
    TASK_TYPE_INTEGRATE_ONLINEREFRESH_SYNC           ,
    TASK_TYPE_INTEGRATE_BIGTXN_SPLITTRAIL            ,
    TASK_TYPE_INTEGRATE_BIGTXN_PARSERTRAIL           ,
    TASK_TYPE_INTEGRATE_BIGTXN_SYNC                  ,
    TASK_TYPE_INTEGRATE_BIGTXN_REBUILD
} task_type;

typedef enum
{
    TASKSLOT_INIT = 0x00,
    TASKSLOT_IDLE = 0x01,
    TASKSLOT_WORK = 0x02,
    TASKSLOT_TERM = 0x03,
    TASKSLOT_EXIT = 0x04
} TASKSLOT_STAT;

typedef struct TASK
{
    task_type type;
} task;

typedef struct TASK_SLOT
{
    int             stat; /* 状态值 */
    task    *task; /* 根据type具体转化 */
} task_slot;

typedef struct TASK_SLOTS
{
    int                 cnt;        /* 总线程数 */
    task_slot   *task_slots; /* 线程信息 */
} task_slots;

extern void taskslot_stat_setidle(task_slot* slot);
extern void taskslot_stat_setwork(task_slot* slot);
extern void taskslot_stat_setterm(task_slot* slot);
extern void taskslot_stat_setexit(task_slot* slot);
extern int taskslot_stat_get(task_slot* slot);
extern task_slots* taskslots_init(void);
extern task_slot* taskslot_init(int cnt);

#endif
