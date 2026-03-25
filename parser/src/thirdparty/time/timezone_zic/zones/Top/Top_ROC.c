#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_ROC[] = {
    (char*)"Rule	Taiwan	1946	only	-	May	15	0:00	1:00	D",
    (char*)"Rule	Taiwan	1946	only	-	Oct	1	0:00	0	S",
    (char*)"Rule	Taiwan	1947	only	-	Apr	15	0:00	1:00	D",
    (char*)"Rule	Taiwan	1947	only	-	Nov	1	0:00	0	S",
    (char*)"Rule	Taiwan	1948	1951	-	May	1	0:00	1:00	D",
    (char*)"Rule	Taiwan	1948	1951	-	Oct	1	0:00	0	S",
    (char*)"Rule	Taiwan	1952	only	-	Mar	1	0:00	1:00	D",
    (char*)"Rule	Taiwan	1952	1954	-	Nov	1	0:00	0	S",
    (char*)"Rule	Taiwan	1953	1959	-	Apr	1	0:00	1:00	D",
    (char*)"Rule	Taiwan	1955	1961	-	Oct	1	0:00	0	S",
    (char*)"Rule	Taiwan	1960	1961	-	Jun	1	0:00	1:00	D",
    (char*)"Rule	Taiwan	1974	1975	-	Apr	1	0:00	1:00	D",
    (char*)"Rule	Taiwan	1974	1975	-	Oct	1	0:00	0	S",
    (char*)"Rule	Taiwan	1979	only	-	Jul	1	0:00	1:00	D",
    (char*)"Rule	Taiwan	1979	only	-	Oct	1	0:00	0	S",
    (char*)"Zone	Asia/Taipei	8:06:00 -	LMT	1896 Jan  1",
    (char*)"			8:00	-	CST	1937 Oct  1",
    (char*)"			9:00	-	JST	1945 Sep 21  1:00",
    (char*)"			8:00	Taiwan	C%sT",
    (char*)"Link	Asia/Taipei		ROC"};
