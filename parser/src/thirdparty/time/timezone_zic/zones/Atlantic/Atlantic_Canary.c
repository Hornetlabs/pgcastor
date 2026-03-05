#include "xk_pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

char* XK_PG_Atlantic_Canary[] =
{
    (char *)"Rule	EU	1977	1980	-	Apr	Sun>=1	 1:00u	1:00	S",
    (char *)"Rule	EU	1977	only	-	Sep	lastSun	 1:00u	0	-",
    (char *)"Rule	EU	1978	only	-	Oct	 1	 1:00u	0	-",
    (char *)"Rule	EU	1979	1995	-	Sep	lastSun	 1:00u	0	-",
    (char *)"Rule	EU	1981	max	-	Mar	lastSun	 1:00u	1:00	S",
    (char *)"Rule	EU	1996	max	-	Oct	lastSun	 1:00u	0	-",
    (char *)"Zone	Atlantic/Canary	-1:01:36 -	LMT	1922 Mar # Las Palmas de Gran C.",
    (char *)"			-1:00	-	CANT	1946 Sep 30  1:00",
    (char *)"			 0:00	-	WET	1980 Apr  6  0:00s",
    (char *)"			 0:00	1:00	WEST	1980 Sep 28  1:00u",
    (char *)"			 0:00	EU	WE%sT"
};
