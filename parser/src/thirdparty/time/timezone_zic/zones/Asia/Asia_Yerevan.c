#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Asia_Yerevan[] =
{
    (char *)"Rule RussiaAsia	1981	1984	-	Apr	1	 0:00	1:00	-",
    (char *)"Rule RussiaAsia	1981	1983	-	Oct	1	 0:00	0	-",
    (char *)"Rule RussiaAsia	1984	1995	-	Sep	lastSun	 2:00s	0	-",
    (char *)"Rule RussiaAsia	1985	2010	-	Mar	lastSun	 2:00s	1:00	-",
    (char *)"Rule RussiaAsia	1996	2010	-	Oct	lastSun	 2:00s	0	-",
    (char *)"Rule Armenia	2011	only	-	Mar	lastSun	 2:00s	1:00	-",
    (char *)"Rule Armenia	2011	only	-	Oct	lastSun	 2:00s	0	-",
    (char *)"Zone	Asia/Yerevan	2:58:00 -	LMT	1924 May  2",
    (char *)"			3:00	-	YERT	1957 Mar",
    (char *)"			4:00 RussiaAsia YER%sT	1991 Mar 31  2:00s",
    (char *)"			3:00 RussiaAsia	YERST	1995 Sep 24  2:00s",
    (char *)"			4:00	-	AMT	1997",
    (char *)"			4:00 RussiaAsia	AM%sT	2011",
    (char *)"			4:00	Armenia	AM%sT"
};
