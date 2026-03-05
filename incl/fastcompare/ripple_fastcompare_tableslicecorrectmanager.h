#ifndef RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_H
#define RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_H

typedef enum RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE
{
    RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_NOP           = 0x00,
    RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_INIT          ,
    RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_WORKING       ,
    RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_EXIT          ,
}ripple_fastcompare_tableslicecorrectmanager_state;

typedef struct RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER
{
    ripple_netclient                        base;
    int                                     state;
    char*                                   conninfo;   /* 数据库连接字符串 */
    PGconn*                                 conn;
    ripple_fastcompare_tablecorrectslice*   correctslice;
    ripple_fastcompare_datacompare*         datacompare;/* 对比结果 */
    ripple_fastcompare_tablecomparecatalog  *catalog;
} ripple_fastcompare_tableslicecorrectmanager;

ripple_fastcompare_tableslicecorrectmanager* riple_fastcompare_tableslicecorrectmanager_init(void);

/* 主流程 */
void* riple_fastcompare_tableslicecorrectmanager_main(void* args);

/* 释放 */
void riple_fastcompare_tableslicecorrectmanager_free(ripple_fastcompare_tableslicecorrectmanager* tableslicecorrmgr);

#endif
