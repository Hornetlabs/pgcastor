#ifndef _XSCSCI_PRESCAN_H_
#define _XSCSCI_PRESCAN_H_

/* Abstract type for lexer's internal state */
typedef struct XSCSCI_PRESCAN xscsci_prescan;

/* Termination states for psql_scan() */
typedef enum XSCSCI_PRESCANRESULT
{
    PSCAN_SEMICOLON,               /* semicolon */
    XSCSCI_PRESCANRESULT_EOL,      /* reached end of parsing */
    XSCSCI_PRESCANRESULT_UNSUPPORT /* unsupported character */
} xscsci_prescanresult;

/* create a parse structure */
extern xscsci_prescan* xscsci_prescan_create(void);

extern void xscsci_prescan_finish(xscsci_prescan* state);

/* set content to parse */
extern void xscsci_prescan_setup(xscsci_prescan* state, const char* line, int line_len);

/* lexical parse entry */
extern xscsci_prescanresult xscsci_prescan_scan(xscsci_prescan* state, xsynch_exbuffer querybuf);

extern void xscsci_prescan_reset(xscsci_prescan* state);

/* line parse finished */
extern void xscsci_prescan_finish(xscsci_prescan* state);

/* destroy */
extern void xscsci_prescan_destroy(xscsci_prescan* state);

#endif
