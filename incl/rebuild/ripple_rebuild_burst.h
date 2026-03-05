#ifndef _RIPPLE_REBUILD_BURST_H
#define _RIPPLE_REBUILD_BURST_H


#define RIPPLE_REBUILD_BURSTNODEFLAG_NO             0x01            /* 没有主键/唯一索引 */
#define RIPPLE_REBUILD_BURSTNODEFLAG_INDEX          0x02            /* 含有主键/唯一索引, index时做 burst消除 */
#define RIPPLE_REBUILD_BURSTNODEFLAG_NOINDEX        0x04            /* 走 PBE 模式 */


typedef enum RIPPLE_REBUILD_BURSTNODETYPE 
{
    RIPPLE_REBUILD_BURSTNODETYPE_NOP                = 0x00,
    RIPPLE_REBUILD_BURSTNODETYPE_DML                ,
    RIPPLE_REBUILD_BURSTNODETYPE_OTHER              
}ripple_rebuild_burstnodetype;

typedef enum RIPPLE_REBUILD_BURSTROWFLAG 
{
    RIPPLE_REBUILD_BURSTROWFLAG_NOP                 = 0x00,
    RIPPLE_REBUILD_BURSTROWFLAG_CHANGECONSKEY       ,
    RIPPLE_REBUILD_BURSTROWFLAG_REMOVEDELETE                    /* 有约束的情况下update拆分出delete需要移除 */
}ripple_rebuild_burstrowflag;

typedef enum RIPPLE_REBUILD_BURSTROWTYPE 
{
    RIPPLE_REBUILD_BURSTROWTYPE_INVALID             = 0x00,
    RIPPLE_REBUILD_BURSTROWTYPE_INSERT              ,
    RIPPLE_REBUILD_BURSTROWTYPE_UPDATE              ,
    RIPPLE_REBUILD_BURSTROWTYPE_DELETE
}ripple_rebuild_burstrowtype;


typedef struct RIPPLE_REBUILD_BURSTCOLUMN
{
    int                                 colno;                  /* 列下标, 在表中的下标 */
    uint32                              coltype;                /* 列类型 */
    char*                               colname;                /* 列名称 */
}ripple_rebuild_burstcolumn;

typedef struct RIPPLE_REBUILD_BURSTROW
{
    ripple_rebuild_burstrowtype         op;                     /* 操作类型  */
    ripple_rebuild_burstrowflag         flag;                   /* 修改的列类型 约束/其他  */
    int                                 missingmapsize;
    int                                 missingcnt;             /* missgin列数量 */
    uint8*                              missingmap;             /* missgin列 */
    uint8                               md5[16];
    struct RIPPLE_REBUILD_BURSTROW*     relatedrow;             /* update拆分出的关联语句insert  ---> delete, delete ---> insert  */
    void*                               row;                    /* 原始数据 xk_pg_praser_translog_tbcol_values */
}ripple_rebuild_burstrow;

typedef struct RIPPLE_REBUILD_BURSTTABLE
{
    Oid                                 oid;
    int                                 keycnt;                 /* 主键个数 */
    uint64                              no;                     /* 编号 */
    char*                               schema;                 /* 模式名 */
    char*                               table;                  /* 表名 */
    ripple_rebuild_burstcolumn*         keys;                   /* 主键列 */
}ripple_rebuild_bursttable;

typedef struct RIPPLE_REBUILD_BURSTNODE
{
    int                                 flag;                   /* 操作模式  */
    ripple_rebuild_burstnodetype        type;                   /* 类型  */
    ripple_rebuild_bursttable           table;                  /* 表信息 */
    dlist*                              dlinsertrows;           /* ripple_rebuild_burstrow */
    dlist*                              dldeleterows;           /* ripple_rebuild_burstrow */
    void*                               stmt;                   /* meta/ddl 等类型, 保留的为: txnstmt */
}ripple_rebuild_burstnode;

typedef struct RIPPLE_REBUILD_BURST
{
    uint64                              number;
    dlist*                              dlbursttable;           /* rebuild_bursttable */
    dlist*                              dlburstnodes;           /* ripple_rebuild_burstnode */
}ripple_rebuild_burst;


/* burstcolumn 初始化 */
extern ripple_rebuild_burstcolumn* ripple_rebuild_burstcolumn_init(int colcnt);

/* burstrow 初始化 */
extern ripple_rebuild_burstrow* ripple_rebuild_burstrow_init(int colcnt);

/* bursttable 初始化 */
extern ripple_rebuild_bursttable* ripple_rebuild_bursttable_init(void);

/* burstnode 初始化 */
extern ripple_rebuild_burstnode* ripple_rebuild_burstnode_init(void);

/* burst 初始化 */
extern ripple_rebuild_burst* ripple_rebuild_burst_init(void);

/* bursttable 比较函数 */
extern int ripple_rebuild_bursttable_cmp(void* s1, void* s2);

/* burstnode 与bursttable 比较函数 */
extern int ripple_rebuild_burstnode_tablecmp(void* s1, void* s2);

/* 获取 burst node节点 */
extern bool ripple_rebuild_burst_getnode(HTAB* hclass,
                                         HTAB* hattrs,
                                         HTAB* hindex,
                                         ripple_rebuild_burst* burst,
                                         ripple_rebuild_burstnode** pburstnode,
                                         ripple_rebuild_bursttable* bursttable);

/* 拆分 update 为 insert/delete */
extern bool ripple_rebuild_burst_decomposeupdate(ripple_rebuild_burstnode* burstnode,
                                                 ripple_rebuild_burstrow** delrow,
                                                 ripple_rebuild_burstrow** insertrow,
                                                 void* rows);

/* 
 * 合并 insert/delete 
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
extern bool ripple_rebuild_burst_mergeinsert(ripple_rebuild_burstnode* pburstnode,
                                             ripple_rebuild_burstrow* insertrow);

/* 
 * 合并 delete/insert
 * 返回 true 说明合并成功, 返回 false 说明合并失败
*/
extern bool ripple_rebuild_burst_mergedelete(ripple_rebuild_burstnode* pburstnode,
                                             ripple_rebuild_burstrow* delrow);
/* update合并 delete/insert */
extern bool ripple_rebuild_burst_updatemergedelete(ripple_rebuild_burstnode* burstnode,
                                                   ripple_rebuild_burstrow* delrow,
                                                   ripple_rebuild_burstrow* updaterow);

/* burstnode 拼接语句 */
extern bool ripple_rebuild_burst_bursts2stmt(ripple_rebuild_burst* burst, ripple_cache_sysdicts* sysdicts, ripple_txn* txn);

/* txn 的内容重组为burst */
extern bool ripple_rebuild_burst_txn2bursts(ripple_rebuild_burst* burst, ripple_cache_sysdicts* sysdicts, ripple_txn* txn);

/* burstcolumn 资源释放 */
extern void ripple_rebuild_burstcolumn_free(ripple_rebuild_burstcolumn* burstcolumn, int colcnt);

/* burstrow 资源释放 */
extern void ripple_rebuild_burstrow_free(void* args);

/* bursttable 资源释放 函数内不是放bursttable */
extern void ripple_rebuild_bursttable_free(void* args);

/* burstnode 资源释放 */
extern void ripple_rebuild_burstnode_free(void* args);

/* burst 资源释放 */
extern void ripple_rebuild_burst_free(void* args);

#endif
