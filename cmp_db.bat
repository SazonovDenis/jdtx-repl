rem Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db1" -database "_test-data\db1.gdb"
rem Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db2" -database "_test-data\db2.gdb"
rem Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db3" -database "_test-data\db3.gdb"


rem diff _test-data\csv\ws1-ulz.csv  _test-data\csv\ws2-ulz.csv _test-data\csv\ws3-ulz.csv
diff _test-data\csv\ws1-all.csv  _test-data\csv\ws2-all.csv _test-data\csv\ws3-all.csv
rem diff _test-data\csv\db1\ _test-data\csv\db2\
