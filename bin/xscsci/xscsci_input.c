#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <pwd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "app_c.h"
#include "xsynch_exbufferdata.h"
#include "xscsci_input.h"
#include "xscsci_tabcomplete.h"

#define XSCSCI_RLHISTORY       ".xscsci_history"
#define XSCSCI_NL_IN_HISTORY   0x01
#define XSCSCI_HISTORY_MAXLINE 500

static int  m_historylinesadded = 0;
static char m_rlhistory[MAXPGPATH] = {0};

#define BEGIN_ITERATE_HISTORY(VARNAME)                                                                                \
    do                                                                                                                \
    {                                                                                                                 \
        HIST_ENTRY* VARNAME;                                                                                          \
        bool        use_prev_;                                                                                        \
                                                                                                                      \
        history_set_pos(0);                                                                                           \
        use_prev_ = (previous_history() != NULL);                                                                     \
        history_set_pos(0);                                                                                           \
        for (VARNAME = current_history(); VARNAME != NULL; VARNAME = use_prev_ ? previous_history() : next_history()) \
        {                                                                                                             \
            (void)0

#define END_ITERATE_HISTORY() \
    }                         \
    }                         \
    while (0)

/*
 * add XSCSCI_NL_IN_HISTORY line
 */
static void xscsci_input_encodehistory(void)
{
    BEGIN_ITERATE_HISTORY(cur_hist);
    {
        char* cur_ptr;

        /* some platforms declare HIST_ENTRY.line as const char * */
        for (cur_ptr = (char*)cur_hist->line; *cur_ptr; cur_ptr++)
        {
            if (*cur_ptr == '\n')
            {
                *cur_ptr = XSCSCI_NL_IN_HISTORY;
            }
        }
    }
    END_ITERATE_HISTORY();
}

/*
 * save history file
 */
static bool xscsci_input_savehistory(char* fname)
{
    int fd = -1;
    int nlines = 0;
    int errnum = 0;

    xscsci_input_encodehistory();

    nlines = Max(XSCSCI_HISTORY_MAXLINE - m_historylinesadded, 0);

    history_truncate_file(fname, nlines);

    /* open file, close immediately after opening, append_history will fail if file doesn't exist */
    fd = open(fname, O_CREAT | O_WRONLY | BINARY, 0600);
    if (0 < fd)
    {
        close(fd);
    }
    nlines = Min(XSCSCI_HISTORY_MAXLINE, m_historylinesadded);

    errnum = append_history(nlines, fname);
    if (errnum == 0)
    {
        return true;
    }

    printf("could not save history to file %s\n", fname);
    return false;
}

/*
 * when xscsci exits, save records to file
 */
static void xscsci_input_finish(void)
{
    /*
     * save file
     */
    xscsci_input_savehistory(m_rlhistory);
    return;
}

/* reverse history records */
static void xscsci_input_decodehistory()
{
    BEGIN_ITERATE_HISTORY(cur_hist);
    {
        char* cur_ptr;

        /* some platforms declare HIST_ENTRY.line as const char * */
        for (cur_ptr = (char*)cur_hist->line; *cur_ptr; cur_ptr++)
        {
            if (*cur_ptr == XSCSCI_NL_IN_HISTORY)
            {
                *cur_ptr = '\n';
            }
        }
    }
    END_ITERATE_HISTORY();
}

/*
 * readline initialization settings
 */
bool xscsci_input_init(void)
{
    struct passwd* pwd = NULL;

    /* set readline completion function and word delimiter */
    xscsci_tabcomplete_initreadline();

    /* readline builtin function initialization */
    rl_initialize();

    /* enable history recording */
    using_history();
    m_historylinesadded = 0;

    /* load history file */
    pwd = getpwuid(geteuid());
    if (NULL == pwd)
    {
        printf("get home path error\n");
        return false;
    }

    snprintf(m_rlhistory, MAXPGPATH, "%s/%s", pwd->pw_dir, XSCSCI_RLHISTORY);
    read_history(m_rlhistory);
    xscsci_input_decodehistory();
    atexit(xscsci_input_finish);
    return true;
}

/*
 * get data through readline
 */
char* xscsci_input_getsinteractive(char* prefix)
{
    char* line = NULL;
    rl_reset_screen_size();
    line = readline(prefix);
    return line;
}

void xscsci_input_appendhistory(const char* s, xsynch_exbuffer historybuf)
{
    xsynch_exbufferdata_appendstr(historybuf, s);
    if (!s[0] || s[strlen(s) - 1] != '\n')
    {
        xsynch_exbufferdata_appendchar(historybuf, '\n');
    }
}

void xscsci_input_sendhistory(xsynch_exbuffer historybuf, char** prevhistorybuf)
{
    int   index = 0;
    char* s = historybuf->data;
    for (index = strlen(s) - 1; index >= 0 && s[index] == '\n'; index--)
    {
        ;
    }
    s[index + 1] = '\0';

    if (NULL != *prevhistorybuf && 0 == strcmp(s, *prevhistorybuf))
    {
        xsynch_exbufferdata_reset(historybuf);
        return;
    }

    if (NULL != *prevhistorybuf)
    {
        free(*prevhistorybuf);
        *prevhistorybuf = NULL;
    }

    *prevhistorybuf = strdup(s);
    add_history(s);
    m_historylinesadded++;
    xsynch_exbufferdata_reset(historybuf);
}
