#ifndef _RIPPLE_LOADTRAILRECORDS_H_
#define _RIPPLE_LOADTRAILRECORDS_H_

typedef struct RIPPLE_LOADTRAILRECOREDS
{
    ripple_loadrecords              loadrecords;

    /* trail 文件兼容版本 */
    int                             compatibility;
    uint64                          fileid;
    uint64                          foffset;
    ripple_recpos                   orgpos;

    /* 存放跨页/文件的 record */
    ripple_recordcross              recordcross;
    mpage*                          mp;

    /* 临时存放完整 record 的链表, 此结构中应该为 TAIL/HEAD/DBMETA 三种类型 */
    dlist*                          remainrecords;
    dlist*                          records;
    ripple_loadpage*                loadpage;
    ripple_loadpageroutine*         loadpageroutine;
} ripple_loadtrailrecords;

/* 初始化 */
ripple_loadtrailrecords* ripple_loadtrailrecords_init(void);

/* 设置加载trail文件的方式 */
bool ripple_loadtrailrecords_setloadpageroutine(ripple_loadtrailrecords* loadrecords, ripple_loadpage_type type);

/* 设置加载的起点 */
void ripple_loadtrailrecords_setloadposition(ripple_loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/* 设置加载的路径 */
bool ripple_loadtrailrecords_setloadsource(ripple_loadtrailrecords* loadrecords, char* source);


/* 加载 records */
bool ripple_loadtrailrecords_load(ripple_loadtrailrecords* loadrecords);

/* 
 * 过滤 record
 *  返回值说明:
 *   true           还需要继续过滤
 *   false          不需要继续过滤
 */
bool ripple_loadtrailrecords_filterfortransbegin(ripple_loadtrailrecords* loadrecords);

/* 关闭文件描述符 */
void ripple_loadtrailrecords_fileclose(ripple_loadtrailrecords* loadrecords);

/*
 * 根据 fileid 和 offset 过滤,小于此值的不需要
*/
void ripple_loadtrailrecords_filter(ripple_loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/*
 * 根据 fileid 和 offset 过滤，但是保留 metadata
 * 返回值说明:
 *   true           还需要继续过滤
 *   false          不需要继续过滤
*/
bool ripple_loadtrailrecords_filterremainmetadata(ripple_loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/* 释放 */
void ripple_loadtrailrecords_free(ripple_loadtrailrecords* loadrecords);

#endif
