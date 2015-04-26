#!/bin/bash
cd project2
cd codebase
cd rbf
make clean
make
cd ../rm
make clean
make

./rmtest_create_tables
# ./rmtest_00
# ./rmtest_01
# ./rbftest2
# ./rbftest3
# ./rbftest4
# ./rbftest5
# ./rbftest6
# ./rbftest7
# ./rbftest8
# ./rbftest8b
# ./rbftest9
# ./rbftest10
# ./rbftest11
# ./rbftest12
# ./rbftest_extra_1
# ./rbftest_private_1
# ./rbftest_private_1b
# ./rbftest_private_2
# ./rbftest_private_3
# ./rbftest_private_4
