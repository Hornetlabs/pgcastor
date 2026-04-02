%{
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include "pgcastor_fe.h"
    #include "xscsci_scan_private.h"

    pgcastor_cmd*                    g_scanparseresult;

%}

%expect 0

%name-prefix="xscsci_scan_yy"
%locations


%union {
        pgcastor_jobkind              kind;
        pgcastor_action               action;
        int                         intval;
        char*                       str;
        pgcastor_cmd*                 node;
        List*                       list;
        pgcastor_rangevar*            rvar;
        pgcastor_job*                 job;
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
                pgcastor_createcmd* createcmd = NULL;
                createcmd = PGCASTOR_MAKECMD(PGCASTOR_CREATECMD);
                createcmd->kind = $2;
                createcmd->name = $3;
                createcmd->job = $4;

                $$ = (pgcastor_cmd*)createcmd;
            }
            ;

alter_progress:
            K_ALTER K_PROGRESS jobname jobaction job_items
            {
                pgcastor_altercmd* altercmd = NULL;
                altercmd = PGCASTOR_MAKECMD(PGCASTOR_ALTERCMD);
                altercmd->kind = PGCASTOR_JOBKIND_PROCESS;
                altercmd->action = $4;
                altercmd->name = $3;
                altercmd->job = $5;

                $$ = (pgcastor_cmd*)altercmd;
            }
            ;

remove_jobconfig:
            K_REMOVE jobkind jobname
            {
                pgcastor_removecmd* removecmd = NULL;
                removecmd = PGCASTOR_MAKECMD(PGCASTOR_REMOVECMD);
                removecmd->kind = $2;
                removecmd->name = $3;

                $$ = (pgcastor_cmd*)removecmd;
            }
            ;

drop_job:
            K_DROP jobkind jobname
            {
                pgcastor_dropcmd* dropcmd = NULL;
                dropcmd = PGCASTOR_MAKECMD(PGCASTOR_DROPCMD);
                dropcmd->kind = $2;
                dropcmd->name = $3;

                $$ = (pgcastor_cmd*)dropcmd;
            }
            ;

init_job:
            K_INIT jobkind jobname
            {
                pgcastor_initcmd* initcmd = NULL;
                initcmd = PGCASTOR_MAKECMD(PGCASTOR_INITCMD);
                initcmd->kind = $2;
                initcmd->name = $3;

                $$ = (pgcastor_cmd*)initcmd;
            }
            ;

edit_job:
            K_EDIT jobkind jobname
            {
                pgcastor_editcmd* editcmd = NULL;
                editcmd = PGCASTOR_MAKECMD(PGCASTOR_EDITCMD);
                editcmd->kind = $2;
                editcmd->name = $3;

                $$ = (pgcastor_cmd*)editcmd;
            }
            ;

start_job:
            K_START jobkind jobname
            {
                pgcastor_startcmd* startcmd = NULL;
                startcmd = PGCASTOR_MAKECMD(PGCASTOR_STARTCMD);
                startcmd->kind = $2;
                startcmd->name = $3;

                $$ = (pgcastor_cmd*)startcmd;
            }
            ;

stop_job:
            K_STOP jobkind jobname
            {
                pgcastor_stopcmd* stopcmd = NULL;
                stopcmd = PGCASTOR_MAKECMD(PGCASTOR_STOPCMD);
                stopcmd->kind = $2;
                stopcmd->name = $3;

                $$ = (pgcastor_cmd*)stopcmd;
            }
            ;

reload_job:
            K_RELOAD jobkind jobname
            {
                pgcastor_reloadcmd* reloadcmd = NULL;
                reloadcmd = PGCASTOR_MAKECMD(PGCASTOR_RELOADCMD);
                reloadcmd->kind = $2;
                reloadcmd->name = $3;

                $$ = (pgcastor_cmd*)reloadcmd;
            }
            ;

info_job:
            K_INFO jobkind jobname
            {
                pgcastor_infocmd* infocmd = NULL;
                infocmd = PGCASTOR_MAKECMD(PGCASTOR_INFOCMD);
                infocmd->kind = $2;
                infocmd->name = $3;

                $$ = (pgcastor_cmd*)infocmd;
            }
            ;

watch_job:
            K_WATCH jobkind jobname time_interval
            {
                pgcastor_watchcmd* watchcmd = NULL;
                watchcmd = PGCASTOR_MAKECMD(PGCASTOR_WATCHCMD);
                watchcmd->kind = $2;
                watchcmd->name = $3;
                watchcmd->interval = $4;

                $$ = (pgcastor_cmd*)watchcmd;
            }
            ;

refresh_job:
            K_REFRESH jobname opt_tables
            {
                pgcastor_refreshcmd* refreshcmd = NULL;
                refreshcmd = PGCASTOR_MAKECMD(PGCASTOR_REFRESHCMD);
                refreshcmd->name = $2;
                refreshcmd->tables = $3;
                $$ = (pgcastor_cmd*)refreshcmd;
            }
    ;

list_job:
            K_LIST
            {
                pgcastor_listcmd* listcmd = NULL;
                listcmd = PGCASTOR_MAKECMD(PGCASTOR_LISTCMD);
                $$ = (pgcastor_cmd*)listcmd;
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
                $$ = pgcastor_rangvar_init($1, $3);
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
                $$ = pgcastor_job_init($1, $2);
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
                    $$ = PGCASTOR_ACTION_ADD;
                }
            | K_REMOVE
                {
                    $$ = PGCASTOR_ACTION_REMOVE;
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
                    $$ = PGCASTOR_JOBKIND_MANAGER;
                }
            | K_CAPTURE
                { 
                    $$ = PGCASTOR_JOBKIND_CAPTURE; 
                }
            | K_INTEGRATE
                {
                    $$ = PGCASTOR_JOBKIND_INTEGRATE;
                }
            | K_PGRECEIVELOG
                {
                    $$ = PGCASTOR_JOBKIND_PGRECEIVELOG;
                }
            | K_PROGRESS
                {
                    $$ = PGCASTOR_JOBKIND_PROCESS;
                }
            | K_ALL
                {
                    $$ = PGCASTOR_JOBKIND_ALL;
                }
            ;

%%

#include "xscsci_scan.c"
