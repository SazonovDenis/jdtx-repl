�����
---------------------------------


������������� sample.* � ����� WEB-INF
sample._app.rt
sample._db-ini.rt
sample.log.properties


������������� cfg/sample.* � ����� WEB-INF/cfg
cfg/sample.srv.json
cfg/sample.ws.json



��������� �������
---------------------------------


�����������:

jc repl_info
jc repl_create

jc repl_add_ws -name:"ws_filial 2"
jc repl_add_ws -name:"ws_filial 3"
...
� �.�.


�����������:

jc repl_setup

jc repl-receive -dir:f:\jdtx-repl\
jc repl-send -dir:f:\jdtx-repl\ -age_from:1 -age_to:999 -mark_done:true


��������� ������� �������
---------------------------------


�����������:

jc repl_info
jc repl_create


�����������:


jc repl_setup
jc repl-send -dir:f:\jdtx-repl\ -age_from:1 -age_to:999 -mark_done:true


������
---------------------------------


-Xms256m -Xmx1024m