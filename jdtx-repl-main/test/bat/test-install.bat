rem @echo off



rem === удаляем старое JadatexSync

call C:\Users\Public\Documents\Jadatex.Sync\unins000.exe /SILENT

rmdir /S /Q C:\Users\Public\Documents\Jadatex.Sync 



rem === удаляем старое Jadatex.PawnShop

del C:\Users\Public\Documents\Jadatex.PawnShop\ini.rt 

del "C:\Program Files (x86)\PawnShop\ini.rt"

del C:\Users\Public\Documents\Jadatex.PawnShop\db.gdb

del C:\Users\Public\Documents\Jadatex.PawnShop\db_demo.gdb



rem === ftp

echo Очистите или удалите каталог *.test_dvsa на ftp
pause 



rem === ставим новое Jadatex.PawnShop

copy db_demo.gdb C:\Users\Public\Documents\Jadatex.PawnShop

rename C:\Users\Public\Documents\Jadatex.PawnShop\db_demo.gdb db.gdb



rem === ставим новый JadatexSync

call %EXE_FILE% /SILENT



rem === настраиваем JadatexSync

rename C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\cfg\sample.srv.json srv.json 

copy setup.test_dvsa.%WS_NAME%.bat C:\Users\Public\Documents\Jadatex.Sync\

del C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\_app.rt

copy _app.rt C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\

cd /d C:\Users\Public\Documents\Jadatex.Sync

call setup.test_dvsa.%WS_NAME%.bat

call jc repl-service-start



rem ===

echo Завершено
pause 
