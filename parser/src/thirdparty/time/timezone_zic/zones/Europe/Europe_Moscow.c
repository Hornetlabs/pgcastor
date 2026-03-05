#include "xk_pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

char* XK_PG_Europe_Moscow[] =
{
    (char *)"Rule	Russia	1917	only	-	Jul	 1	23:00	1:00	MST  # Moscow Summer Time",
    (char *)"Rule	Russia	1917	only	-	Dec	28	 0:00	0	MMT  # Moscow Mean Time",
    (char *)"Rule	Russia	1918	only	-	May	31	22:00	2:00	MDST # Moscow Double Summer Time",
    (char *)"Rule	Russia	1918	only	-	Sep	16	 1:00	1:00	MST",
    (char *)"Rule	Russia	1919	only	-	May	31	23:00	2:00	MDST",
    (char *)"Rule	Russia	1919	only	-	Jul	 1	 0:00u	1:00	MSD",
    (char *)"Rule	Russia	1919	only	-	Aug	16	 0:00	0	MSK",
    (char *)"Rule	Russia	1921	only	-	Feb	14	23:00	1:00	MSD",
    (char *)"Rule	Russia	1921	only	-	Mar	20	23:00	2:00	+05",
    (char *)"Rule	Russia	1921	only	-	Sep	 1	 0:00	1:00	MSD",
    (char *)"Rule	Russia	1921	only	-	Oct	 1	 0:00	0	-",
    (char *)"Rule	Russia	1981	1984	-	Apr	 1	 0:00	1:00	S",
    (char *)"Rule	Russia	1981	1983	-	Oct	 1	 0:00	0	-",
    (char *)"Rule	Russia	1984	1995	-	Sep	lastSun	 2:00s	0	-",
    (char *)"Rule	Russia	1985	2010	-	Mar	lastSun	 2:00s	1:00	S",
    (char *)"Rule	Russia	1996	2010	-	Oct	lastSun	 2:00s	0	-",
    (char *)"Zone Europe/Moscow	 2:30:17 -	LMT	1880",
    (char *)"			 2:30:17 -	MMT	1916 Jul  3 # Moscow Mean Time",
    (char *)"			 2:31:19 Russia	GMT	1919 Jul  1  0:00u",
    (char *)"			 3:00	Russia	GMT	1921 Oct",
    (char *)"			 3:00	Russia	MSK/MSD	1922 Oct",
    (char *)"			 2:00	-	EET	1930 Jun 21",
    (char *)"			 3:00	Russia	MSK/MSD	1991 Mar 31  2:00s",
    (char *)"			 2:00	Russia	EE%sT	1992 Jan 19  2:00s",
    (char *)"			 3:00	Russia	MSK/MSD	2011 Mar 27  2:00s",
    (char *)"			 4:00	-	MSK	2014 Oct 26  2:00s",
    (char *)"			 3:00	-	MSK"
};
