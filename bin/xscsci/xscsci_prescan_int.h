#ifndef XSCSCI_PRESCAN_INT_H_
#define XSCSCI_PRESCAN_INT_H_

/*
 * internal header file for prescan
 */

typedef struct XSCSCI_PRESCAN
{
    yyscan_t        scanner;
    /* output buffer */
    xsynch_exbuffer outputbuf;

    int             startstate;

    YY_BUFFER_STATE scanbufhandle;
    char*           scanbuf;

    const char*     scanline; /* current input line at outer level */

} xscsci_prescan;

extern void xscsci_prescan_emit(xscsci_prescan* state, const char* txt, int len);

extern YY_BUFFER_STATE xscsci_prescan_prebuffer(xscsci_prescan* state, const char* txt, int len, char** txtcopy);

#endif
