rem @echo off

rem �।����������, �� Jadatex.PawnShop 㦥 ��⠭������, �����塞 ⮫쪮 ����
rem ��⠫�� JadatexSync ��ࠥ� � ��⠭�������� ������



rem === 㤠�塞 ��஥: JadatexSync

call C:\Users\Public\Documents\Jadatex.Sync\unins000.exe /SILENT

rmdir /S /Q C:\Users\Public\Documents\Jadatex.Sync 



rem === 㤠�塞 ��஥: Jadatex.PawnShop

del C:\Users\Public\Documents\Jadatex.PawnShop\ini.rt 

del "C:\Program Files (x86)\PawnShop\ini.rt"

del C:\Users\Public\Documents\Jadatex.PawnShop\db.gdb

del C:\Users\Public\Documents\Jadatex.PawnShop\db_demo.gdb



rem === ftp

echo ����� ��⠫�� *.test_dvsa �� ftp
pause 



rem === �⠢�� �����: Jadatex.PawnShop

if not "%WS_NAME%"=="srv" goto srv1_false


:srv1_true

copy db_work.gdb C:\Users\Public\Documents\Jadatex.PawnShop

rename C:\Users\Public\Documents\Jadatex.PawnShop\db_work.gdb db.gdb

goto srv1_end


:srv1_false

copy db_demo.gdb C:\Users\Public\Documents\Jadatex.PawnShop

rename C:\Users\Public\Documents\Jadatex.PawnShop\db_demo.gdb db.gdb

:srv1_end



rem === �⠢�� �����: JadatexSync

call %EXE_FILE% /SILENT



rem === ����ࠨ���� JadatexSync

if not "%WS_NAME%"=="srv" goto srv2_false


:srv2_true

echo ����ன�� �ࢥ�

rename C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\cfg\sample.srv.json srv.json 

del C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\_app.rt

copy _app.srv.rt C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\


:srv2_false

echo ����ன�� ࠡ�祩 �⠭樨 %WS_NAME%

copy setup.test_dvsa.%WS_NAME%.bat C:\Users\Public\Documents\Jadatex.Sync\

cd /d C:\Users\Public\Documents\Jadatex.Sync

call setup.test_dvsa.%WS_NAME%.bat

call jc repl-service-start



rem ===

echo ���⮢�� �।� ��� %WS_NAME% ࠧ�����
pause 
