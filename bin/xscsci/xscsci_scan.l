%{
    /* 头文件 */
    #include "ripple_c.h"
    #include "xscsci_scansup.h"
    #include "xscsci_gram.h"

    typedef struct XSCSCI_SCANKEYWORD2IDENT
    {
        char*               key;
        int                 len;
        int                 ident;
    } xscsci_scankeyword2ident;

    static xscsci_scankeyword2ident m_keyword2ident[] =
    {
        {"NOP",             0,              0},
        {"create",          6,              K_CREATE},
        {"alter",           5,              K_ALTER},
        {"remove",          6,              K_REMOVE},
        {"drop",            4,              K_DROP},
        {"init",            4,              K_INIT},
        {"edit",            4,              K_EDIT},
        {"start",           5,              K_START},
        {"stop",            4,              K_STOP},
        {"reload",          6,              K_RELOAD},
        {"info",            4,              K_INFO},
        {"manager",         7,              K_MANAGER},
        {"capture",         7,              K_CAPTURE},
        {"pump",            4,              K_PUMP},
        {"collector",       9,              K_COLLECTOR},
        {"integrate",       9,              K_INTEGRATE},
        {"hgreceivelog",    12,             K_HGRECEIVELOG},
        {"pgreceivelog",    12,             K_PGRECEIVELOG},
        {"progress",        8,              K_PROGRESS},
        {"refresh",         7,              K_REFRESH},
        {"list",            4,              K_LIST},
        {"all",             3,              K_ALL},
        {"watch",           5,              K_WATCH},
        {"add",             3,              K_ADD}
    };

    static int                          m_litbuflen;
    static int                          m_litbufsize;
    static char*                        m_litbuf;
    static int                          m_columnno;
    static int                          m_lineno;
    static YY_BUFFER_STATE              m_scanbufhandle;

    static char* xscsci_litbufdup(void);
    static void xscsci_startlit(void);
    static void xscsci_addlit(char *ytext, int yleng);
    static bool xscsci_can_keyword(char* text, int tlen, int* ident);

%}


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
%option prefix="xscsci_scan_yy"

%x xd

dquote          \"
xdstart         {dquote}
xdstop          {dquote}
xdinside        [^"]+

/* 数字 */
digit           [0-9]+

/* 单词组成 */
ident_start     [A-Za-z]
ident_cont      [A-Za-z0-9]

/* 单词匹配 */
identifier      {ident_start}{ident_cont}*

/* 其它字符, 报错 */
other           .

%%

[\n]            {
                    m_columnno = 0;
                    printf("m_lineno:%d\n", m_lineno);
                    m_lineno++;
                }

[\t]            {
                    m_columnno++;
                }

" "             {
                    m_columnno++;
                }

";"             {
                    m_columnno += 1;
                    return ';'; 
                }

"."             {
                    m_columnno += 1;
                    return '.';
                }

","             {
                    m_columnno += 1;
                    return COMMA;
                }

{digit}+        {
                    m_columnno += strlen(yytext);
                    yylval.intval = strtol(yytext, NULL, 10);
                    return ICONST;
                }


{xdstart}       {
                    BEGIN(xd);
                    m_columnno += strlen(yytext);
                    xscsci_startlit();
                    xscsci_addlit(yytext, yyleng);
                }

<xd>{xdstop}    {
                    BEGIN(INITIAL);
                    m_columnno += strlen(yytext);
                    xscsci_addlit(yytext, yyleng);
                    yylval.str = xscsci_litbufdup();
                    return IDENT;
                }

<xd>{xdinside}  {
                    m_columnno += strlen(yytext);
                    xscsci_addlit(yytext, yyleng);
                }

{identifier}    {
                    int lident = 0;
                    int len = strlen(yytext);

                    /* 查看是否为 KEYWORD */
                    m_columnno += len;
                    if (true == xscsci_can_keyword(yytext, len, &lident))
                    {
                        return lident;
                    }
                    yylval.str = xscsci_scansup_downcasetruncateident(yytext, len, true);
                    return IDENT;
                }

<xd><<EOF>>     {
                    yyterminate();
                }


{other}         {
                    yyterminate();
                }

<<EOF>>         {
                    yyterminate();
                }

%%

/* C 代码 */

static void xscsci_startlit(void)
{
    m_litbufsize = 1024;
    m_litbuf = malloc(m_litbufsize);
    if (NULL == m_litbuf)
    {
        printf("out of memory\n");
        exit(1);
    }

    memset(m_litbuf, '\0', m_litbufsize);
    m_litbuflen = 0;
}

static void xscsci_addlit(char *ytext, int yleng)
{
    bool balloc = false;
    char* cptr = NULL;
    while ((m_litbuflen + yleng) >= m_litbufsize)
    {
        m_litbufsize *= 2;
        balloc = true;
    }

    if (true == balloc)
    {
        cptr = realloc(m_litbuf, m_litbufsize);
        if (NULL == cptr)
        {
            printf("out of memory\n");
            exit(1);
        }
        memset(cptr + m_litbuflen, 0, m_litbufsize - m_litbuflen);
        m_litbuf = cptr;
    }

    memcpy(m_litbuf + m_litbuflen, ytext, yleng);
    m_litbuflen += yleng;
}

static char* xscsci_litbufdup(void)
{
    return m_litbuf;
}

void yyerror(const char *message)
{
    /*
     * 1、找到错误的行号
     * 2、输出列内容
     * 3、在列内容的下方标识出错误的内容
     */
    int lineno = 1;
    int endpos = 0;
    int spacecnt = 0;
    char lineinfo[128] = { 0 };
    char* inputstr = NULL;
    char* cptr = NULL;

    /* 找到错误的行号 */
    inputstr = m_scanbufhandle->yy_buf_pos;
    cptr = inputstr;
     while(1)
    {
        if (m_lineno == lineno)
        {
            break;
        }

        if ('\n' == *cptr)
        {
            cptr++;
            lineno++;
            continue;
        }
        cptr++;
    }

    endpos = m_columnno - yyleng;
    if (0 > endpos)
    {
        printf("ERROR: %s, %s, error keyword:%s\n", message, m_scanbufhandle->yy_ch_buf, yytext);
        return;
    }

    /* 换算正确的 endpos */
    snprintf(lineinfo, 128, "LINE %d: ", m_lineno);
    /* 输出错误的详细信息 */
    printf("ERROR: %s at or near '%s' \n", message, yytext);
    printf("%s", lineinfo);
    for (spacecnt = 0; spacecnt < endpos; spacecnt++)
    {
        printf("%c", *cptr);
        cptr++;
    }
    printf("%s\n", yytext);

    spacecnt += strlen(lineinfo);
    while (0 < spacecnt)
    {
        printf(" ");
        spacecnt--;
    }
    printf("^\n");
}

static bool xscsci_can_keyword(char* text, int tlen, int* ident)
{
    int index = 0;
    for (index = 1;index < (sizeof(m_keyword2ident)/sizeof(xscsci_scankeyword2ident)); index++)
    {
        if (tlen != m_keyword2ident[index].len)
        {
            continue;
        }

        if (0 == strcasecmp(text, m_keyword2ident[index].key))
        {
            *ident = m_keyword2ident[index].ident;
            return true;
        }
    }

    return false;
}

/* 初始化 */
void xscsci_scan_init(const char *str)
{
    Size slen                = strlen(str);
    char* scanbuf           = NULL;

    m_lineno = 1;
    m_columnno = 0;
    if (YY_CURRENT_BUFFER)
        yy_delete_buffer(YY_CURRENT_BUFFER);

    scanbuf = (char *) malloc(slen + 2);
    memcpy(scanbuf, str, slen);
    scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
    m_scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);
}

/* 结束 */
void xscsci_scan_finish(void)
{
    yy_delete_buffer(m_scanbufhandle);
    m_scanbufhandle = NULL;
    printf("invoke end xscsci_scan_finish\n");
}
