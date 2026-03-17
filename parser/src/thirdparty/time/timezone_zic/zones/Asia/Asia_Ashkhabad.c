#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Asia_Ashkhabad[] =
{
    (char *)"Rule RussiaAsia	1981	1984	-	Apr	1	 0:00	1:00	-",
    (char *)"Rule RussiaAsia	1981	1983	-	Oct	1	 0:00	0	-",
    (char *)"Rule RussiaAsia	1984	1995	-	Sep	lastSun	 2:00s	0	-",
    (char *)"Rule RussiaAsia	1985	2010	-	Mar	lastSun	 2:00s	1:00	-",
    (char *)"Rule RussiaAsia	1996	2010	-	Oct	lastSun	 2:00s	0	-",
    (char *)"Zone	Asia/Ashgabat	3:53:32 -	LMT	1924 May  2 # or Ashkhabad",
    (char *)"			4:00	-	ASHT	1930 Jun 21",
    (char *)"			5:00 RussiaAsia	ASH%sT	1991 Mar 31  2:00",
    (char *)"			4:00 RussiaAsia	TM%sT	1992 Jan 19  2:00",
    (char *)"			5:00	-	TMT",
    (char *)"Link	Asia/Ashgabat		Asia/Ashkhabad"
};
