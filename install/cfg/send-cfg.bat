rem @echo off

rem set cfg_path=%~dp0
set cfg_path=C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\cfg\

rem cd ..

@echo -------------------------
@echo %cfg_path%
@echo -------------------------

rem ��� ������� �������� ������ ��������� ����������
call jc repl_set_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json
call jc repl_set_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_srv.json


rem ��������� �� ������� ������� ����� ��������� �������������
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:1
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:2
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:3
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:4
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:5
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:6
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:7
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:8
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:9
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:10
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:11
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:12
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:13
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:14
call jc repl_send_cfg -cfg:cfg_decode -file:%cfg_path%decode_strategy_194.json -ws:15


rem ��������� �� ������� ������� ����� ������� ����������
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_srv.json -ws:1
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:2
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:3
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:4
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:5
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:6
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:7
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:8
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:9
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:10
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:11
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:12
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:13
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:14
call jc repl_send_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json -ws:15


pause Done send cfg


