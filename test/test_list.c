#include "ripple_app_incl.h"
#include "utils/list/list_func.h"

static char *make_str(char *format)
{
    char *str = malloc(strlen(format) + 1);
    strcpy(str, format);
    return str;
}

int main(void)
{
    List *test_list = NULL;
    int index_list = 0;
    ListCell   *cell;
    char temp_str[16] = {'\0'};

    printf("----------do insert----------\n");
    for (index_list = 0; index_list < 10; index_list++)
    {
        sprintf(temp_str, "list: %d", index_list);
        test_list = lappend(test_list, make_str(temp_str));
        memset(temp_str, 0, 16);
    }
    printf("----------do foreach----------\n");
    foreach(cell, test_list)
    {
        char *str = (char *) lfirst(cell);
        printf("%s\n", str);
    }
    index_list = 0;
    printf("----------do delete----------\n");
    for ((cell) = list_head(test_list); (cell) != ((void *)0);)
    {
        char *str = (char *) lfirst(cell);
        cell = (cell)->next;
        if (index_list >= 3 && index_list <= 5)
        {
            list_delete(test_list, str);
            free(str);
        }
        index_list++;
    }
    foreach(cell, test_list)
    {
        char *str = (char *) lfirst(cell);
        printf("%s\n", str);
    }

    printf("----------do destory----------\n");
    list_free_deep(test_list);
    return 0;
}