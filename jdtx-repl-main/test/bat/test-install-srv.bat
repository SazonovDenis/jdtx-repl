rem @echo off

SET EXE_FILE=JadatexSync-311DEV.jre.exe
SET WS_NAME=srv



rem ===

rmdir /S /Q s:\t
mkdir s:\t
copy %~dp0* s:\t



rem ===

call test-install.bat