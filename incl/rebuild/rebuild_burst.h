#ifndef _REBUILD_BURST_H
#define _REBUILD_BURST_H


#define REBUILD_BURSTNODEFLAG_NO             0x01            /* 没有主键/唯一索引 */
#define REBUILD_BURSTNODEFLAG_INDEX          0x02            /* 含有主键/唯一索引, index时做 burst消除 */
#define REBUILD_BURSTNODEFLAG_NOINDEX        0x04            /* 走 PBE 模式 */


typedef enum REBUILD_BURSTNODETYPE 
{
    REBUILD_BURSTNODETYPE_NOP                = 0x00,
    REBUILD_BURSTNODETYPE_DML                ,
    REBUILD_BURSTNODETYPE_OTHER              
}rebuild_burstnodetype;

typedef enum REBUILD_BURSTROWFLAG 
{
    REBUILD_BURSTROWFLAG_NOP                 = 0x00,
    REBUILD_BURSTROWFLAG_CHANGECONSKEY       ,
    REBUILD_BURSTROWFLAG_REMOVEDELETE                    /* 有约束的情况下update拆分出delete需要移除 */
}rebuild_burstrowflag;

typedef enum REBUILD_BURSTROWTYPE 
{
    REBUILD_BURSTROWTYPE_INVALID             = 0x00,
    REBUILD_BURSTROWTYPE_INSERT              ,
    REBUILD_BURSTROWTYPE_UPDATE              ,
    REBUILD_BURSTROWTYPE_DELETE
}rebuild_burstrowtype;


typedef struct REBUILD_BURSTCOLUMN
{
    int                                 colno;                  /* 列下标, 在表中的下标 */
    uint32                              coltype;                /* 列类型 */
    char*                               colname;                /* 列名称 */
}rebuild_burstcolumn;

typedef struct REBUILD_BURSTROW
{
    rebuild_burstrowtype         op;                     /* 操作类型  */
    rebuild_burstrowflag         flag;                   /* 修改的列类型 约束/其他  */
    int                                 missingmapsize;
    int                                 missingcnt;             /* missgin列数量 */
    uint8*                              missingmap;             /* missgin列 */
    uint8                               md5[16];
    struct REBUILD_BURSTROW*     relatedrow;             /* update拆分出的关联语句insert  ---> delete, delete ---> insert  */
    void*                               row;                    /* 原始数据 xk_pg_praser_translog_tbcol_values */
}rebuild_burstrow;

typedef struct REBUILD_BURSTTABLE
{
    Oid                                 oid;
    int                                 keycnt;                 /* 主键个数 */
    uint64                              no;                     /* 编号 */
    char*                               schema;                 /* 模式名 */
    char*                               table;                  /* 表名 */
    rebuild_burstcolumn*         keys;                   /* 主键列 */
}rebuild_bursttable;

typedef struct REBUILD_BURSTNODE
{
    int                                 flag;                   /* 操作模式  */
    rebuild_burstnodetype        type;                   /* 类型  */
    rebuild_bursttable           table;                  /* 表信息 */
    dlist*                              dlinsertrows;           /* rebuild_burstrow */
    dlist*                              dldeleterows;           /* rebuild_burstrow */
    void*                               stmt;                   /* meta/ddl 等类型, 保留的为: txnstmt */
}rebuild_burstnode;

typedef struct REBUILD_BURST
{
    uint64                              number;
    dlist*                              dlbursttable;           /* rebuild_bursttable */
    dlist*                              dlburstnodes;           /* rebuild_burstnode */
}rebuild_burst;


/* burstcolumn 初始化 */
extern rebuild_burstcolumn* rebuild_burstcolumn_init(int colcnt);

/* burstrow 初始化 */
extern rebuild_burstrow* rebuild_burstrow_init(int colcnt);

/* bursttable 初始化 */
extern rebuild_bursttable* rebuild_bursttable_init(void);

/* burstnode 初始化 */
extern rebuild_burstnode* rebuild_burstnode_init(void);

/* burst 初始化 */
extern rebuild_burst* rebuild_burst_init(void);

/* bursttable 比较函数 */
extern int rebuild_bursttable_cmp(void* s1, void* s2);

/* burstnode 与bursttable 比较函数 */
extern int rebuild_burstnode_tablecmp(void* s1, void* s2);

/* 获取 burst node节点 */
extern bool rebuild_burst_getnode(HTAB* hclass,
                                         HTAB* hattrs,
                                         HTAB* hindex,
                                         rebuild_burst* burst,
                                         rebuild_burstnode** pburstnode,
                                         rebuild_bursttable* bursttable);

/* 拆分 update 为 insert/delete */
extern bool rebuild_burst_decomposeupdate(rebuild_burstnode* burstnode,
                                                 rebuild_burstrow** delrow,
                                                 rebuild_burstrow** insertrow,
                                                 void* rows);

/* 
 * 合并 insert/delete 
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
extern bool rebuild_burst_mergeinsert(rebuild_burstnode* pburstnode,
                                             rebuild_burstrow* insertrow);

/* 
 * 合并 delete/insert
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
extern bool rebuild_burst_mergedelete(rebuild_burstnode* pburstnode,
                                             rebuild_burstrow* delrow);
/*
 * update合并 delete/insert
 * 参数说明: delrow update拆分出的befor值
 * 
 * 返回值说明: 返回 true 说明合并成功, 返回 false 说明合并失败
 *          ：in_updaterow合并成功返回匹配上的insertrow，不成功updaterow
 *          ：error false执行失败退出，true执行成功
*/
extern bool rebuild_burst_updatemergedelete(rebuild_burstnode* burstnode,
                                                   rebuild_burstrow* delrow,
                                                   rebuild_burstrow** updaterow,
                                                   bool* error);

/* burstnode 拼接语句 */
extern bool rebuild_burst_bursts2stmt(rebuild_burst* burst, cache_sysdicts* sysdicts, txn* txn);

/* txn 的内容重组为burst */
extern bool rebuild_burst_txn2bursts(rebuild_burst* burst, cache_sysdicts* sysdicts, txn* txn);

/* burstcolumn 资源释放 */
extern void rebuild_burstcolumn_free(rebuild_burstcolumn* burstcolumn, int colcnt);

/* burstrow 资源释放 */
extern void rebuild_burstrow_free(void* args);

/* bursttable 资源释放 函数内不是放bursttable */
extern void rebuild_bursttable_free(void* args);

/* burstnode 资源释放 */
extern void rebuild_burstnode_free(void* args);

/* burst 资源释放 */
extern void rebuild_burst_free(void* args);

#endif
