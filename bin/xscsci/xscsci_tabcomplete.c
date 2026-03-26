#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <stdarg.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "app_c.h"
#include "xscsci_tabcomplete.h"

#define XSCSCI_WORD_BREAKS " "
#define completion_matches rl_completion_matches
#define XSCSCI_MATCHANY    NULL

#define XSCSCI_VA_ARGS_NARGS_(_01, \
                              _02, \
                              _03, \
                              _04, \
                              _05, \
                              _06, \
                              _07, \
                              _08, \
                              _09, \
                              _10, \
                              _11, \
                              _12, \
                              _13, \
                              _14, \
                              _15, \
                              _16, \
                              _17, \
                              _18, \
                              _19, \
                              _20, \
                              _21, \
                              _22, \
                              _23, \
                              _24, \
                              _25, \
                              _26, \
                              _27, \
                              _28, \
                              _29, \
                              _30, \
                              _31, \
                              _32, \
                              _33, \
                              _34, \
                              _35, \
                              _36, \
                              _37, \
                              _38, \
                              _39, \
                              _40, \
                              _41, \
                              _42, \
                              _43, \
                              _44, \
                              _45, \
                              _46, \
                              _47, \
                              _48, \
                              _49, \
                              _50, \
                              _51, \
                              _52, \
                              _53, \
                              _54, \
                              _55, \
                              _56, \
                              _57, \
                              _58, \
                              _59, \
                              _60, \
                              _61, \
                              _62, \
                              _63, \
                              N,   \
                              ...) \
    (N)

#define XSCSCI_VA_ARGS_NARGS(...)      \
    XSCSCI_VA_ARGS_NARGS_(__VA_ARGS__, \
                          63,          \
                          62,          \
                          61,          \
                          60,          \
                          59,          \
                          58,          \
                          57,          \
                          56,          \
                          55,          \
                          54,          \
                          53,          \
                          52,          \
                          51,          \
                          50,          \
                          49,          \
                          48,          \
                          47,          \
                          46,          \
                          45,          \
                          44,          \
                          43,          \
                          42,          \
                          41,          \
                          40,          \
                          39,          \
                          38,          \
                          37,          \
                          36,          \
                          35,          \
                          34,          \
                          33,          \
                          32,          \
                          31,          \
                          30,          \
                          29,          \
                          28,          \
                          27,          \
                          26,          \
                          25,          \
                          24,          \
                          23,          \
                          22,          \
                          21,          \
                          20,          \
                          19,          \
                          18,          \
                          17,          \
                          16,          \
                          15,          \
                          14,          \
                          13,          \
                          12,          \
                          11,          \
                          10,          \
                          9,           \
                          8,           \
                          7,           \
                          6,           \
                          5,           \
                          4,           \
                          3,           \
                          2,           \
                          1,           \
                          0)

/* first keyword */
static const char* const m_commands[] = {"create",
                                         "alter",
                                         "remove",
                                         "drop",
                                         "init",
                                         "edit",
                                         "start",
                                         "stop",
                                         "reload",
                                         "info",
                                         "watch",
                                         "refresh",
                                         "help",
                                         "exit",
                                         "quit",
                                         "list",
                                         NULL};

/*
 * keywords after create progress
 */
static const char* const m_createprogresscommands[] = {"capture", NULL};

/*
 * keywords after create progress
 */
static const char* const m_createcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", "progress", NULL};

/*
 * content after alter keyword
 */
/* alter */
static const char* const m_altercommands[] = {"progress", NULL};

/* alter progress */
static const char* const m_alterprogresscommands[] = {"add", "remove", NULL};

/* content after remove keyword */
static const char* const m_removecommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", NULL};

/* content after drop keyword */
static const char* const m_dropcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", "progress", NULL};

/* content after init keyword */
static const char* const m_initcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", NULL};

/* content after edit keyword */
static const char* const m_editcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", NULL};

/* content after start keyword */
static const char* const m_startcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", "all", NULL};

/* content after stop keyword */
static const char* const m_stopcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", "all", NULL};

/* content after reload keyword */
static const char* const m_reloadcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", NULL};

/* content after info keyword */
static const char* const m_infocommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", "progress", "all", NULL};

/* content after watch keyword */
static const char* const m_watchcommands[] = {
    "manager", "pgreceivelog", "capture", "integrate", "progress", "all", NULL};

/* content after help keyword */

static const char*        m_tabcompleteconst = NULL;
static const char* const* m_commandslist = NULL;

/* word matching */
static bool xscsci_tabcomplete_wordmatches(const char* pattern, const char* word)
{
    size_t      wordlen = 0;
    const char* cptr = NULL;

    if (NULL == pattern)
    {
        return true;
    }

    wordlen = strlen(word);
    while (1)
    {
        cptr = pattern;
        while ('\0' != *cptr)
        {
            cptr++;
        }

        /* matched */
        if (wordlen == (cptr - pattern) && 0 == strncasecmp(word, pattern, wordlen))
        {
            return true;
        }

        break;
    }

    return false;
}

/* check if exists in previous input */
static bool xscsci_tabcomplete_matchesimpl(int prevwordscnt, char** prevwords, int narg, ...)
{
    int         index = 0;
    const char* arg = NULL;
    va_list     args;

    /* force association, if not equal then exit */
    if (prevwordscnt != narg)
    {
        return false;
    }

    va_start(args, narg);

    /* traverse input and match with prevwords */
    for (index = 0; index < narg; index++)
    {
        arg = va_arg(args, const char*);

        if (false == xscsci_tabcomplete_wordmatches(arg, prevwords[narg - index - 1]))
        {
            va_end(args);
            return false;
        }
    }

    va_end(args);
    return true;
}

#define XSCSCI_TABCOMPLETE_MATCHES(...) \
    xscsci_tabcomplete_matchesimpl(     \
        prevwordcnt, prevwords, XSCSCI_VA_ARGS_NARGS(__VA_ARGS__), __VA_ARGS__)

/*
 * get count of words already entered
 */
static char** xscsci_tabcomplete_getprevwords(int point, char** buffer, int* nwords)
{
    int    index = 0;
    int    wstart = 0;
    int    wend = 0;
    int    wordsfound = 0;
    char*  buf = NULL;
    char*  outptr = NULL;
    char** prevwords = NULL;

    /*
     * history records also need to be added here, this scenario is not supported yet
     */
    buf = rl_line_buffer;

    if (0 == point)
    {
        *nwords = 0;
        *buffer = NULL;
        return NULL;
    }

    /* pre-allocate word space */
    prevwords = (char**)malloc(point * sizeof(char*));
    if (NULL == prevwords)
    {
        printf("tab complete get prev words out of memory %s\n", strerror(errno));
        exit(1);
    }
    memset(prevwords, '\0', point * sizeof(char*));

    /* pre-allocate buffer space */
    *buffer = outptr = (char*)malloc(point * 2);
    if (NULL == *buffer)
    {
        printf("tab complete get prev words out of memory %s\n", strerror(errno));
        exit(1);
    }
    memset(*buffer, '\0', point * 2);

    /* match to the start of the last word */
    for (index = point - 1; index >= 0; index--)
    {
        if (strchr(XSCSCI_WORD_BREAKS, buf[index]))
        {
            break;
        }
    }
    point = index;

    while (0 <= point)
    {
        wend = -1;

        /* filter out spaces */
        for (index = point; index >= 0; index--)
        {
            if (!isspace((unsigned char)buf[index]))
            {
                wend = index;
                break;
            }
        }

        if (0 > wend)
        {
            break;
        }

        /* word start position */
        for (wstart = wend; wstart > 0; wstart--)
        {
            if (strchr(XSCSCI_WORD_BREAKS, buf[wstart - 1]))
            {
                break;
            }
        }

        prevwords[wordsfound++] = outptr;
        index = wend - wstart + 1;
        memcpy(outptr, &buf[wstart], index);
        outptr += index;
        *outptr++ = '\0';

        /* recalculate starting position */
        point = wstart - 1;
    }

    *nwords = wordsfound;
    return prevwords;
}

/* generate specified constant completion */
static char* xscsci_tabcomplete_fromconst(const char* text, int state)
{
    if (0 == state)
    {
        return strdup(m_tabcompleteconst);
    }
    else
    {
        return NULL;
    }
}

/*
 * completion when user has no input
 */
static char* xscsci_tabcomplete_matches(const char* text, int state)
{
    static int  index;
    const char* item = NULL;
    if (0 == state)
    {
        index = 0;
    }

    while ((item = m_commandslist[index++]))
    {
        if (NULL == text || ' ' == text[0] || '\0' == text[0])
        {
            return strdup(item);
        }
        else if (strlen(text) <= strlen(item))
        {
            if (0 == strncasecmp(text, item, strlen(text)))
            {
                return strdup(item);
            }
        }
    }

    return NULL;
}

/*
 * tab key completion
 */
static char** xscsci_tabcomplete_completion(const char* text, int start, int end)
{
    int    prevwordcnt = 0;
    char** prevwords = NULL;
    char*  wordsbuffer = NULL;
    char** matches = NULL;

    (void)end;

    rl_completion_append_character = ' ';

    prevwords = xscsci_tabcomplete_getprevwords(start, &wordsbuffer, &prevwordcnt);
    if (NULL == prevwords)
    {
        m_commandslist = m_commands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("CREATE"))
    {
        m_commandslist = m_createcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("ALTER"))
    {
        m_commandslist = m_altercommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("REMOVE"))
    {
        m_commandslist = m_removecommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("DROP"))
    {
        m_commandslist = m_dropcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("INIT"))
    {
        m_commandslist = m_initcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("EDIT"))
    {
        m_commandslist = m_editcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("START"))
    {
        m_commandslist = m_startcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("STOP"))
    {
        m_commandslist = m_stopcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("RELOAD"))
    {
        m_commandslist = m_reloadcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("INFO"))
    {
        m_commandslist = m_infocommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("WATCH"))
    {
        m_commandslist = m_watchcommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("ALTER", "PROGRESS"))
    {
        /* TODO: future extension, get data from manager */
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("ALTER", "PROGRESS", XSCSCI_MATCHANY))
    {
        m_commandslist = m_alterprogresscommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }
    else if (true == XSCSCI_TABCOMPLETE_MATCHES("CREATE", "PROGRESS", XSCSCI_MATCHANY))
    {
        m_commandslist = m_createprogresscommands;
        matches = completion_matches(text, xscsci_tabcomplete_matches);
    }

    if (NULL == matches)
    {
        m_tabcompleteconst = "";
        matches = completion_matches(text, xscsci_tabcomplete_fromconst);
        rl_completion_append_character = '\0';
    }

    if (NULL != wordsbuffer)
    {
        free(wordsbuffer);
    }

    if (NULL != prevwords)
    {
        free(prevwords);
    }

    return matches;
}

/* readline initialization */
void xscsci_tabcomplete_initreadline(void)
{
    rl_readline_name = "xscsci";
    rl_attempted_completion_function = xscsci_tabcomplete_completion;
    rl_basic_word_break_characters = XSCSCI_WORD_BREAKS;
}
