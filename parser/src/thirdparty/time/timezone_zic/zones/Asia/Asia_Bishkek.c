#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Asia_Bishkek[] =
{
    (char *)"Rule RussiaAsia	1981	1984	-	Apr	1	 0:00	1:00	-",
    (char *)"Rule RussiaAsia	1981	1983	-	Oct	1	 0:00	0	-",
    (char *)"Rule RussiaAsia	1984	1995	-	Sep	lastSun	 2:00s	0	-",
    (char *)"Rule RussiaAsia	1985	2010	-	Mar	lastSun	 2:00s	1:00	-",
    (char *)"Rule RussiaAsia	1996	2010	-	Oct	lastSun	 2:00s	0	-",
    (char *)"Rule	Kyrgyz	1992	1996	-	Apr	Sun>=7	0:00s	1:00	-",
    (char *)"Rule	Kyrgyz	1992	1996	-	Sep	lastSun	0:00	0	-",
    (char *)"Rule	Kyrgyz	1997	2005	-	Mar	lastSun	2:30	1:00	-",
    (char *)"Rule	Kyrgyz	1997	2004	-	Oct	lastSun	2:30	0	-",
    (char *)"Zone	Asia/Bishkek	4:58:24 -	LMT	1924 May  2",
    (char *)"			5:00	-	FRUT	1930 Jun 21",
    (char *)"			6:00 RussiaAsia FRU%sT	1991 Mar 31  2:00s",
    (char *)"			5:00 RussiaAsia	FRUST	1991 Aug 31  2:00",
    (char *)"			5:00	Kyrgyz	KG%sT	2005 Aug 12",
    (char *)"			6:00	-	KGT"
};
