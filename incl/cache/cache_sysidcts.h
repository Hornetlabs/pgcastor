#ifndef _CACHE_SYSDICTS_H
#define _CACHE_SYSDICTS_H

typedef struct CACHE_SYSDICTS
{
    /* 字典表缓存 */
    HTAB*                       by_class;           /* pg_class 表信息,             oid */
    HTAB*                       by_attribute;       /* pg_attribute, hash+list, atttrelid, list属性表信息       */
    HTAB*                       by_type;            /* pg_type 表信息 */
    HTAB*                       by_proc;            /* pg_proc 表信息 */

    /* tablespace 表信息 */
    HTAB*                       by_tablespace;      /* pg_tablespace hash 表， key = id */

    /* namespace 表信息 */
    HTAB*                       by_namespace;       /* pg_namespace hash 表, key = id */

    /* range */
    HTAB*                       by_range;           /* pg_range  hash 表 key =rangtypid 对应关系 */

    /* enum */
    HTAB*                       by_enum;            /* pg_enum, hash+list, enumtypid      */

    /* operator */
    HTAB*                       by_operator;        /* pg_operator, 表信息,             oid*/

    /* authid */
    HTAB*                       by_authid;          /* pg_authid, 表信息,             oid*/

    /* database */
    HTAB*                       by_database;        /* pg_database, 表信息,             oid*/

    /* constraint */
    HTAB*                       by_constraint;        /* pg_database, 表信息,             oid*/

    /* datname2oid */
    HTAB*                       by_datname2oid;     /* pg_database, 表信息,             datname*/

    HTAB*                       by_relfilenode;     /* relfilenode 与 oid 对应关系, key: relfilenode, entry: relfilenode2oid */

    /* index */
    HTAB*                       by_index;           /* pg_index, 索引信息,             oid*/
} cache_sysdicts;

/* 加载数据 */
void cache_sysdictsload(void** ref_sysdicts);

cache_sysdicts *cache_sysdicts_integrate_init(void);

void cache_sysdicts_free(void* args);

/* 将修改后的字典表缓存信息落盘 */
void sysdictscache_write(cache_sysdicts* sysdicts, XLogRecPtr redolsn);

/* 按照 relfilenode 的结构构建hash表,用于在解析的过程中快速查找relfilenode到表oid */
HTAB* cache_sysdicts_buildrelfilenode2oid(Oid dbid, void* data);

/* 在事务提交或回滚时，将历史的字典表结构释放 */
void cache_sysdicts_txnsysdicthisfree(List* sysdicthis);

/* 将 sysdict 字典表结构释放 */
void cache_sysdicts_catalogdatafreevoid(void* args);

/* 在事务提交, 将历史的字典表应用到缓存中 */
void cache_sysdicts_txnsysdicthis2cache(cache_sysdicts* sysdicts, List* sysdicthis);

/*
 * 根据class清理系统表中相关数据
*/
void cache_sysdicts_clearsysdicthisbyclass(cache_sysdicts* sysdicts, ListCell* lc);

/*
 * 单个系统表应用
*/
void cache_sysdicts_txnsysdicthisitem2cache(cache_sysdicts* sysdicts, ListCell* lc);

#endif
