@echo off
rem ����� ��⭨�� � ���⮬ ०���

SET CURR_DIR=%~dp0
cd /d %CURR_DIR%

rem ��⮢�� 䠩� ����᪠ � ���⮬ ०���
echo Set WshShell = CreateObject("WScript.Shell") > start.vbs
echo WshShell.Run "cmd.exe /c run-jc.bat", 1, false >> start.vbs
 
rem ����᪠�� �� �믮������ � ���⮬ ०���, ���� �믮������ �� ���.
cscript.exe //b //Logo start.vbs
 
rem ��ࠥ� �६���� 䠩��
del /f /q start.vbs

