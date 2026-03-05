#ifndef _XSCSCI_PRESCAN_H_
#define _XSCSCI_PRESCAN_H_


/* Abstract type for lexer's internal state */
typedef struct XSCSCI_PRESCAN xscsci_prescan;

/* Termination states for psql_scan() */
typedef enum XSCSCI_PRESCANRESULT
{
    PSCAN_SEMICOLON,                    /* 分号                */
    XSCSCI_PRESCANRESULT_EOL,           /* 解析到最后           */
    XSCSCI_PRESCANRESULT_UNSUPPORT      /* 不支持的字符         */
} xscsci_prescanresult;

/* 创建一个解析结构 */
extern xscsci_prescan* xscsci_prescan_create(void);

extern void xscsci_prescan_finish(xscsci_prescan* state);

/* 设置解析的内容 */
extern void xscsci_prescan_setup(xscsci_prescan* state,
                                 const char *line,
                                 int line_len);

/* 词法解析入口 */
extern xscsci_prescanresult xscsci_prescan_scan(xscsci_prescan* state,
                                                xsynch_exbuffer querybuf);

extern void xscsci_prescan_reset(xscsci_prescan* state);

/* 行解析结束 */
extern void xscsci_prescan_finish(xscsci_prescan* state);

/* 销毁 */
extern void xscsci_prescan_destroy(xscsci_prescan* state);

#endif
