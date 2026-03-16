#ifndef RIPPLE_BIGTXN_H
#define RIPPLE_BIGTXN_H

typedef struct RIPPLE_BIGTXN
{
    FullTransactionId       xid;        /* 事务号 */
    ripple_ffsmgr_fdata*    fdata;      /* 序列化结构 */
    List*                   txndicts;   /* 系统字典 */
    ripple_file_buffer      fbuffer;    /* 保存的临时页面，临时保存 */
} ripple_bigtxn;

extern bool ripple_bigtxn_reset(ripple_bigtxn* bigtxn);
extern void ripple_bigtxn_clean(ripple_bigtxn *htxn);

#endif
