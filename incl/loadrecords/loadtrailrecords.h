#ifndef _LOADTRAILRECORDS_H_
#define _LOADTRAILRECORDS_H_

typedef struct LOADTRAILRECOREDS
{
    loadrecords              loadrecords;

    /* trail 文件兼容版本 */
    int                             compatibility;
    uint64                          fileid;
    uint64                          foffset;
    recpos                   orgpos;

    /* 存放跨页/文件的 record */
    recordcross              recordcross;
    mpage*                          mp;

    /* 临时存放完整 record 的链表, 此结构中应该为 TAIL/HEAD/DBMETA 三种类型 */
    dlist*                          remainrecords;
    dlist*                          records;
    loadpage*                loadpage;
    loadpageroutine*         loadpageroutine;
} loadtrailrecords;

/* 初始化 */
loadtrailrecords* loadtrailrecords_init(void);

/* 设置加载trail文件的方式 */
bool loadtrailrecords_setloadpageroutine(loadtrailrecords* loadrecords, loadpage_type type);

/* 设置加载的起点 */
void loadtrailrecords_setloadposition(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/* 设置加载的路径 */
bool loadtrailrecords_setloadsource(loadtrailrecords* loadrecords, char* source);


/* 加载 records */
bool loadtrailrecords_load(loadtrailrecords* loadrecords);

/* 
 * 过滤 record
 *  返回值说明:
 *   true           还需要继续过滤
 *   false          不需要继续过滤
 */
bool loadtrailrecords_filterfortransbegin(loadtrailrecords* loadrecords);

/* 关闭文件描述符 */
void loadtrailrecords_fileclose(loadtrailrecords* loadrecords);

/*
 * 根据 fileid 和 offset 过滤,小于此值的不需要
*/
void loadtrailrecords_filter(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/*
 * 根据 fileid 和 offset 过滤，但是保留 metadata
 * 返回值说明:
 *   true           还需要继续过滤
 *   false          不需要继续过滤
*/
bool loadtrailrecords_filterremainmetadata(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/* 释放 */
void loadtrailrecords_free(loadtrailrecords* loadrecords);

#endif
