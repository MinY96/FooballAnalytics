
=IFERROR(
    LET(
        data, FILTER('PE 부문'!$A$6:$X$1000, ISNUMBER('PE 부문'!$X$6:$X$1000)),
        sorted, SORT(data, 24, 1),
        n, MIN('PE 부문'!$AA$5, ROWS(sorted)),
        IF(n<1, "", INDEX(sorted, SEQUENCE(n), {1,2,3,4,5,23,24}))
    ),
    ""
)
