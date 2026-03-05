%{

/* LCOV_EXCL_START */
#include "ripple_c.h"
#include "xsynch_exbufferdata.h"
#include "xscsci_prescan_int.h"
#include "xscsci_prescan.h"

typedef int YYSTYPE;

#define YY_EXTRA_TYPE xscsci_prescan*


#define LEXRES_EOL              1       /* 行解析结束 */
#define LEXRES_SEMI             2       /* 分号结束 */
#define LEXRES_UNSUPPORTCHAR    3       /* 不支持的字符 */

#define ECHO xscsci_prescan_emit(m_curstate, yytext, yyleng)

%}

/* 多线程 */
%option reentrant

/* bison 桥接 */
%option bison-bridge

/* 声明扫描的单位是8bit */
%option 8bit

/* 不匹配的输出回显到输出设备上 */
%option nodefault

/* 非交互式的 */
%option never-interactive

/* 禁用 yyin */
%option noinput

/* 禁用 flex 生成 unput 函数 */
%option nounput

/* 到达解析尾部时, 返回 0 */
%option noyywrap

/* 声明生成文件的前缀 */
%option prefix="xscsci_prescan_yy"

%x xd

dquote          \"
xdstart         {dquote}
xdstop          {dquote}
xdinside        [^"]+

/*
 * 匹配规则 
*/
space           [ \t\n\r\f]
whitespace      ({space}+)

/* 单词组成 */
ident_start     [A-Za-z]
ident_cont      [A-Za-z0-9]

/* 单词匹配 */
identifier      {ident_start}{ident_cont}*

/* 其它字符, 报错 */
other           .

%%

%{
    xscsci_prescan* m_curstate = yyextra;
    xsynch_exbuffer m_outputbuf = m_curstate->outputbuf;
    BEGIN(m_curstate->startstate);
%}

{whitespace}    {
                    if (!(0 == m_outputbuf->len))
                    {
                        ECHO;
                    }
                }

{identifier}    {
                    ECHO;
                }

";"             {
                    ECHO;
                    /* 一行命令的结束 */
                    m_curstate->startstate = YY_START;
                    return LEXRES_SEMI;
                }

"."             {
                    ECHO;
                }

","             {
                    ECHO;
                }


{xdstart}       {
                    BEGIN(xd);
                    ECHO;
                }

<xd>{xdinside}  {
                    ECHO;
                }


<xd>{xdstop}    {
                    BEGIN(INITIAL);
                    ECHO;
                }


{other}         {
                    ECHO;

                    /* 获取行号和列号 */
                    return LEXRES_UNSUPPORTCHAR;
                }

<<EOF>>         {
                        m_curstate->startstate = YY_START;
                        return LEXRES_EOL;
                }

%%

void xscsci_prescan_reset(xscsci_prescan* state)
{
    state->startstate = INITIAL;
}

void xscsci_prescan_emit(xscsci_prescan* state, const char *txt, int len)
{
    xsynch_exbufferdata_appendbinary(state->outputbuf, txt, len);
}

xscsci_prescan* xscsci_prescan_create(void)
{
    xscsci_prescan* state = NULL;

    state = (xscsci_prescan *)malloc(sizeof(xscsci_prescan));
    if (NULL == state)
    {
        return NULL;
    }

    yylex_init(&state->scanner);

    yyset_extra(state, state->scanner);

    xscsci_prescan_reset(state);

    return state;
}

YY_BUFFER_STATE xscsci_prescan_prebuffer(xscsci_prescan* state,
                                         const char *txt,
                                         int len,
                                         char **txtcopy)
{
    char* newtxt        = NULL;

    /* Flex wants two \0 characters after the actual data */
    newtxt = malloc(len + 2);
    *txtcopy = newtxt;
    newtxt[len] = newtxt[len + 1] = YY_END_OF_BUFFER_CHAR;
    memcpy(newtxt, txt, len);
    return yy_scan_buffer(newtxt, len + 2, state->scanner);
}

void xscsci_prescan_setup(xscsci_prescan* state,
                          const char *line,
                          int line_len)
{
    /* Set up flex input buffer with appropriate translation and padding */
    state->scanbufhandle = xscsci_prescan_prebuffer(state,
                                                    line,
                                                    line_len,
                                                    &state->scanbuf);

    state->scanline = line;
}

xscsci_prescanresult xscsci_prescan_scan(xscsci_prescan* state, xsynch_exbuffer querybuf)
{
    int lexresult               = 0;
    xscsci_prescanresult result = 0;

    state->outputbuf = querybuf;

    yy_switch_to_buffer(state->scanbufhandle, state->scanner);

    lexresult = yylex(NULL, state->scanner);

    /* lexresult 设置返回值 */
    switch (lexresult)
    {
        case LEXRES_EOL:
            result = XSCSCI_PRESCANRESULT_EOL;
            break;
        case LEXRES_SEMI:
            result = PSCAN_SEMICOLON;
            break;
        case LEXRES_UNSUPPORTCHAR:
            result= XSCSCI_PRESCANRESULT_UNSUPPORT;
            break;
        default:
            fprintf(stderr, "invalid yylex result\n");
            exit(1);
    }

    return result;
}


void xscsci_prescan_finish(xscsci_prescan* state)
{
    if (NULL != state->scanbufhandle)
    {
        yy_delete_buffer(state->scanbufhandle, state->scanner);
    }

    state->scanbufhandle = NULL;
    if (NULL != state->scanbuf)
    {
        free(state->scanbuf);
    }
    state->scanbuf = NULL;
}


void xscsci_prescan_destroy(xscsci_prescan* state)
{
    xscsci_prescan_finish(state);

    xscsci_prescan_reset(state);

    yylex_destroy(state->scanner);

    free(state);
}
