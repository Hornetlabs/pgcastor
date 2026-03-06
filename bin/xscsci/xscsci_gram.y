%{
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include "xsynch_fe.h"
    #include "xscsci_scan_private.h"

    xsynch_cmd*                    g_scanparseresult;

%}

%expect 0

%name-prefix="xscsci_scan_yy"
%locations


%union {
        xsynch_jobkind              kind;
        xsynch_action               action;
        int                         intval;
        char*                       str;
        xsynch_cmd*                 node;
        List*                       list;
        xsynch_rangevar*            rvar;
        xsynch_job*                 job;
}

/* 非关键字 tokens */
%token <str> IDENT
%token <intval> ICONST
%token T_WORD

/* 关键字 tokens */
%token K_CREATE
%token K_ALTER
%token K_REMOVE
%token K_DROP
%token K_INIT
%token K_EDIT
%token K_START
%token K_STOP
%token K_RELOAD
%token K_INFO
%token K_MANAGER
%token K_CAPTURE
%token K_INTEGRATE
%token K_PGRECEIVELOG
%token K_PROGRESS
%token K_REFRESH
%token K_LIST
%token K_ALL
%token K_WATCH
%token K_ADD
%token COMMA


%type <node>    command
%type <node>    create_job alter_progress remove_jobconfig drop_job init_job
                edit_job start_job stop_job reload_job info_job watch_job refresh_job list_job

%type <str>     jobname
%type <kind>    jobkind
%type <action>  jobaction
%type <intval>  time_interval
%type <list>    opt_tables tables
%type <rvar>    table_el
%type <list>    job_items
%type <job>     job_item

%%

firstcmd: command semicolon
                {
                    g_scanparseresult = $1;
                }
                ;

semicolon: ';'
            | /* 空 */
            ;

command:
            create_job
            | alter_progress
            | remove_jobconfig
            | drop_job
            | init_job
            | edit_job
            | start_job
            | stop_job
            | reload_job
            | info_job
            | watch_job
            | refresh_job
            | list_job
            ;

create_job:
            K_CREATE jobkind jobname job_items
            {
                xsynch_createcmd* createcmd = NULL;
                createcmd = XSYNCH_MAKECMD(XSYNCH_CREATECMD);
                createcmd->kind = $2;
                createcmd->name = $3;
                createcmd->job = $4;

                $$ = (xsynch_cmd*)createcmd;
            }
            ;

alter_progress:
            K_ALTER K_PROGRESS jobname jobaction job_items
            {
                xsynch_altercmd* altercmd = NULL;
                altercmd = XSYNCH_MAKECMD(XSYNCH_ALTERCMD);
                altercmd->kind = XSYNCH_JOBKIND_PROCESS;
                altercmd->action = $4;
                altercmd->name = $3;
                altercmd->job = $5;

                $$ = (xsynch_cmd*)altercmd;
            }
            ;

remove_jobconfig:
            K_REMOVE jobkind jobname
            {
                xsynch_removecmd* removecmd = NULL;
                removecmd = XSYNCH_MAKECMD(XSYNCH_REMOVECMD);
                removecmd->kind = $2;
                removecmd->name = $3;

                $$ = (xsynch_cmd*)removecmd;
            }
            ;

drop_job:
            K_DROP jobkind jobname
            {
                xsynch_dropcmd* dropcmd = NULL;
                dropcmd = XSYNCH_MAKECMD(XSYNCH_DROPCMD);
                dropcmd->kind = $2;
                dropcmd->name = $3;

                $$ = (xsynch_cmd*)dropcmd;
            }
            ;

init_job:
            K_INIT jobkind jobname
            {
                xsynch_initcmd* initcmd = NULL;
                initcmd = XSYNCH_MAKECMD(XSYNCH_INITCMD);
                initcmd->kind = $2;
                initcmd->name = $3;

                $$ = (xsynch_cmd*)initcmd;
            }
            ;

edit_job:
            K_EDIT jobkind jobname
            {
                xsynch_editcmd* editcmd = NULL;
                editcmd = XSYNCH_MAKECMD(XSYNCH_EDITCMD);
                editcmd->kind = $2;
                editcmd->name = $3;

                $$ = (xsynch_cmd*)editcmd;
            }
            ;

start_job:
            K_START jobkind jobname
            {
                xsynch_startcmd* startcmd = NULL;
                startcmd = XSYNCH_MAKECMD(XSYNCH_STARTCMD);
                startcmd->kind = $2;
                startcmd->name = $3;

                $$ = (xsynch_cmd*)startcmd;
            }
            ;

stop_job:
            K_STOP jobkind jobname
            {
                xsynch_stopcmd* stopcmd = NULL;
                stopcmd = XSYNCH_MAKECMD(XSYNCH_STOPCMD);
                stopcmd->kind = $2;
                stopcmd->name = $3;

                $$ = (xsynch_cmd*)stopcmd;
            }
            ;

reload_job:
            K_RELOAD jobkind jobname
            {
                xsynch_reloadcmd* reloadcmd = NULL;
                reloadcmd = XSYNCH_MAKECMD(XSYNCH_RELOADCMD);
                reloadcmd->kind = $2;
                reloadcmd->name = $3;

                $$ = (xsynch_cmd*)reloadcmd;
            }
            ;

info_job:
            K_INFO jobkind jobname
            {
                xsynch_infocmd* infocmd = NULL;
                infocmd = XSYNCH_MAKECMD(XSYNCH_INFOCMD);
                infocmd->kind = $2;
                infocmd->name = $3;

                $$ = (xsynch_cmd*)infocmd;
            }
            ;

watch_job:
            K_WATCH jobkind jobname time_interval
            {
                xsynch_watchcmd* watchcmd = NULL;
                watchcmd = XSYNCH_MAKECMD(XSYNCH_WATCHCMD);
                watchcmd->kind = $2;
                watchcmd->name = $3;
                watchcmd->interval = $4;

                $$ = (xsynch_cmd*)watchcmd;
            }
            ;

refresh_job:
            K_REFRESH jobname opt_tables
            {
                xsynch_refreshcmd* refreshcmd = NULL;
                refreshcmd = XSYNCH_MAKECMD(XSYNCH_REFRESHCMD);
                refreshcmd->name = $2;
                refreshcmd->tables = $3;
                $$ = (xsynch_cmd*)refreshcmd;
            }
    ;

list_job:
            K_LIST
            {
                xsynch_listcmd* listcmd = NULL;
                listcmd = XSYNCH_MAKECMD(XSYNCH_LISTCMD);
                $$ = (xsynch_cmd*)listcmd;
            }
            ;


opt_tables: tables
                {
                    $$ = $1;
                }
    ;


tables:     tables COMMA table_el
                {
                    $$ = lappend($1, $3);
                }
            | table_el
                {
                    $$ = list_make1($1);
                }
    ;


table_el:   jobname '.' jobname
            {
                $$ = xsynch_rangvar_init($1, $3);
            }
    ;

job_items:  job_items COMMA job_item
                {
                    $$ = lappend($1, $3);
                }
            | job_item
                {
                    $$ = list_make1($1);
                }
            |   /* EMPTY */
                { 
                    $$ = NIL; 
                }
    ;

job_item:   jobkind jobname
            {
                $$ = xsynch_job_init($1, $2);
            }
    ;

time_interval:
            ICONST
                {
                    $$=$1;
                }
            |
                {
                    $$=5;
                }
            ;

jobaction:
            K_ADD
                {
                    $$ = XSYNCH_ACTION_ADD;
                }
            | K_REMOVE
                {
                    $$ = XSYNCH_ACTION_REMOVE;
                }
            ;

jobname:
            IDENT
                {
                    $$ = strdup($1);
                }
                ;

jobkind:
            K_MANAGER
                {
                    $$ = XSYNCH_JOBKIND_MANAGER;
                }
            | K_CAPTURE
                { 
                    $$ = XSYNCH_JOBKIND_CAPTURE; 
                }
            | K_INTEGRATE
                {
                    $$ = XSYNCH_JOBKIND_INTEGRATE;
                }
            | K_PGRECEIVELOG
                {
                    $$ = XSYNCH_JOBKIND_PGRECEIVELOG;
                }
            | K_PROGRESS
                {
                    $$ = XSYNCH_JOBKIND_PROCESS;
                }
            | K_ALL
                {
                    $$ = XSYNCH_JOBKIND_ALL;
                }
            ;

%%

#include "xscsci_scan.c"
