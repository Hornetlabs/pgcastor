#include "xk_pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

char* XK_PG_Asia_Baku[] =
{
    (char *)"Rule RussiaAsia	1981	1984	-	Apr	1	 0:00	1:00	-",
    (char *)"Rule RussiaAsia	1981	1983	-	Oct	1	 0:00	0	-",
    (char *)"Rule RussiaAsia	1984	1995	-	Sep	lastSun	 2:00s	0	-",
    (char *)"Rule RussiaAsia	1985	2010	-	Mar	lastSun	 2:00s	1:00	-",
    (char *)"Rule RussiaAsia	1996	2010	-	Oct	lastSun	 2:00s	0	-",
    (char *)"Rule	EUAsia	1981	max	-	Mar	lastSun	 1:00u	1:00	S",
    (char *)"Rule	EUAsia	1979	1995	-	Sep	lastSun	 1:00u	0	-",
    (char *)"Rule	EUAsia	1996	max	-	Oct	lastSun	 1:00u	0	-",
    (char *)"Rule	Azer	1997	2015	-	Mar	lastSun	 4:00	1:00	-",
    (char *)"Rule	Azer	1997	2015	-	Oct	lastSun	 5:00	0	-",
    (char *)"Zone	Asia/Baku	3:19:24 -	LMT	1924 May  2",
    (char *)"			3:00	-	BAKT	1957 Mar",
    (char *)"			4:00 RussiaAsia BAK%sT	1991 Mar 31  2:00s",
    (char *)"			3:00 RussiaAsia	AZ%sT	1992 Sep lastSun  2:00s",
    (char *)"			4:00	-	AZT	1996",
    (char *)"			4:00	EUAsia	AZ%sT	1997",
    (char *)"			4:00	Azer	AZ%sT"
};
