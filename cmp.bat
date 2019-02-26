Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db" -database "_test-data\db.gdb"
Z:\PawnShop\Prepare_db.exe -cmd databaseToSnapshot -csv_dir_name "_test-data\csv\db2" -database "_test-data\db2.gdb"

diff _test-data\csv\db\RegionTip.csv _test-data\csv\db2\RegionTip.csv 
rem diff csv\db\Region.csv csv\db2\Region.csv 
