#ifndef _RIPPLE_TASK_SLOT_H
#define _RIPPLE_TASK_SLOT_H

typedef enum RIPPLE_TASK_TYPE
{
    RIPPLE_TASKTYPE_NOP = 0x00,
    RIPPLE_TASKTYPE_REFRESHSHARDING                         ,
    RIPPLE_TASKTYPE_REFRESHP2CSHARDING                      ,
    RIPPLE_TASKTYPE_REFRESHSHARDING2DB                      ,
    RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_PARSERWAL         ,
    RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_SPLITWAL          ,
    RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_SERIAL            ,
    RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_WRITE             ,
    RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_SPLITTRAIL     ,
    RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_PARSERTRAIL    ,
    RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_REBUILD        ,
    RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_SYNC           ,
    RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_SPLITTRAIL          ,
    RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_PARSERTRAIL         ,
    RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_SERIAL              ,
    RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_CLIENT              ,
    RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_P2CSHARDING         ,
    RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_SPLITTRAIL            ,
    RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_PARSERTRAIL           ,
    RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_SYNC                  ,
    RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_REBUILD               ,
    RIPPLE_TASK_TYPE_FASTCOMPARE_TABLESLICE                 ,
    RIPPLE_TASK_TYPE_PUMP_BIGTXN_SPLITTRAIL                 ,
    RIPPLE_TASK_TYPE_PUMP_BIGTXN_PARSERTRAIL                ,
    RIPPLE_TASK_TYPE_PUMP_BIGTXN_SERIAL                     ,
    RIPPLE_TASK_TYPE_PUMP_BIGTXN_CLIENT
} ripple_task_type;

typedef enum
{
    RIPPLE_TASKSLOT_INIT = 0x00,
    RIPPLE_TASKSLOT_IDLE = 0x01,
    RIPPLE_TASKSLOT_WORK = 0x02,
    RIPPLE_TASKSLOT_TERM = 0x03,
    RIPPLE_TASKSLOT_EXIT = 0x04
} RIPPLE_TASKSLOT_STAT;

typedef struct RIPPLE_TASK
{
    ripple_task_type type;
} ripple_task;

typedef struct RIPPLE_TASK_SLOT
{
    int             stat; /* 状态值 */
    ripple_task    *task; /* 根据type具体转化 */
} ripple_task_slot;

typedef struct RIPPLE_TASK_SLOTS
{
    int                 cnt;        /* 总线程数 */
    ripple_task_slot   *task_slots; /* 线程信息 */
} ripple_task_slots;

extern void ripple_taskslot_stat_setidle(ripple_task_slot* slot);
extern void ripple_taskslot_stat_setwork(ripple_task_slot* slot);
extern void ripple_taskslot_stat_setterm(ripple_task_slot* slot);
extern void ripple_taskslot_stat_setexit(ripple_task_slot* slot);
extern int ripple_taskslot_stat_get(ripple_task_slot* slot);
extern ripple_task_slots* ripple_taskslots_init(void);
extern ripple_task_slot* ripple_taskslot_init(int cnt);

#endif
