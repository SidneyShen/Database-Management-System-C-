#!/bin/bash
# cd project1
# cd codebase
# cd rbf
# make clean
# make
# ./rbftest1
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


#!/bin/bash
# cd project2
# cd codebase
# cd rbf
# make clean
# make
# cd ../rm
# make clean
# make

# ./rmtest_delete_tables
# ./rmtest_create_tables
# ./rmtest_00
# ./rmtest_01
# ./rmtest_02
# ./rmtest_03
# ./rmtest_04
# ./rmtest_05
# ./rmtest_06
# ./rmtest_07
# ./rmtest_08
# ./rmtest_09
# ./rmtest_10
# ./rmtest_11
# ./rmtest_12
# ./rmtest_13
# ./rmtest_13b
# ./rmtest_14
# ./rmtest_15
# ./rmtest_extra_1
# ./rmtest_extra_2



#!/bin/bash
unzip $1.zip
cd $1
cd codebase
cd rbf
make clean
make
cd ../rm
make clean
make
./rmtest_create_tables
./rmtest_00
./rmtest_01
./rmtest_02
./rmtest_03
./rmtest_04
./rmtest_05
./rmtest_06
./rmtest_07
./rmtest_08
./rmtest_09
./rmtest_10
./rmtest_11
./rmtest_12
./rmtest_13
./rmtest_13b
./rmtest_14
./rmtest_15
