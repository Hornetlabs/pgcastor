#include "ripple_app_incl.h"
#include "utils/string/stringinfo.h"

int main(void)
{
    StringInfo str = makeStringInfo();

    appendStringInfo(str, "test1");
    printf("stringinfo: %s\n", str->data);

    appendStringInfo(str, " test2");
    printf("stringinfo: %s\n", str->data);

    deleteStringInfo(str);
    return 0;
}