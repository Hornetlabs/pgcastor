#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_America_Miquelon[] =
{
    (char *)"Rule	Canada	1918	only	-	Apr	14	2:00	1:00	D",
    (char *)"Rule	Canada	1918	only	-	Oct	27	2:00	0	S",
    (char *)"Rule	Canada	1942	only	-	Feb	 9	2:00	1:00	W # War",
    (char *)"Rule	Canada	1945	only	-	Aug	14	23:00u	1:00	P # Peace",
    (char *)"Rule	Canada	1945	only	-	Sep	30	2:00	0	S",
    (char *)"Rule	Canada	1974	1986	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Canada	1974	2006	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Canada	1987	2006	-	Apr	Sun>=1	2:00	1:00	D",
    (char *)"Rule	Canada	2007	max	-	Mar	Sun>=8	2:00	1:00	D",
    (char *)"Rule	Canada	2007	max	-	Nov	Sun>=1	2:00	0	S",
    (char *)"Zone America/Miquelon	-3:44:40 -	LMT	1911 May 15 # St Pierre",
    (char *)"			-4:00	-	AST	1980 May",
    (char *)"			-3:00	-	PMST	1987",
    (char *)"			-3:00	Canada	PM%sT"
};
