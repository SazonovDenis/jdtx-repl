@echo off
rem Запуск батника в скрытом режиме

SET CURR_DIR=%~dp0
cd /d %CURR_DIR%

rem Готовим файл запуска в скрытом режиме
echo Set WshShell = CreateObject("WScript.Shell") > start.vbs
echo WshShell.Run "cmd.exe /c run-jc.bat", 1, false >> start.vbs
 
rem Запускает на выполнение в скрытом режиме, конца выполнения не ждём.
cscript.exe //b //Logo start.vbs
 
rem Стираем временные файлы
del /f /q start.vbs

