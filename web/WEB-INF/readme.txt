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

jc repl_add_ws -id:1 -name:"Sever"
jc repl_add_ws -id:2 -name:"ws filial 2"
jc repl_add_ws -id:3 -name:"ws filial 3"
...
� �.�.


�����������:

jc repl_snapshot
jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



��������� ������� �������
---------------------------------


�����������:

jc repl_info
jc repl_create


�����������:


jc repl_snapshot
jc repl-sync -dir:f:\jdtx-repl\ -mark:true



������
---------------------------------


-Xms256m -Xmx1024m