#ifndef BIGTXN_H
#define BIGTXN_H

typedef struct BIGTXN
{
    FullTransactionId       xid;        /* 事务号 */
    ffsmgr_fdata*    fdata;      /* 序列化结构 */
    List*                   txndicts;   /* 系统字典 */
    file_buffer      fbuffer;    /* 保存的临时页面，临时保存 */
} bigtxn;

extern bool bigtxn_reset(bigtxn* bigtxn);
extern void bigtxn_clean(bigtxn *htxn);

#endif
