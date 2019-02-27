Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db1" -database "_test-data\db1.gdb"
Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db2" -database "_test-data\db2.gdb"

rem diff _test-data\csv\db1\RegionTip.csv _test-data\csv\db2\RegionTip.csv 
diff _test-data\csv\db1\Ulz.csv _test-data\csv\db2\Ulz.csv 
rem diff _test-data\csv\db1\ _test-data\csv\db2\


rem Z:\PawnShop\Prepare_db.exe -cmd snapshotToDatabase -csv_dir_name "Z:\jdtx-repl\jdtx-repl-main\test\etalon\csv_empty" -database "Z:\jdtx-repl\jdtx-repl-main\test\etalon\db_empty.gdb"