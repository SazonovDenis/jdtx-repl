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

jc repl-info
jc repl-create -id:1

jc repl-add-ws -id:1 -name:"Sever"
jc repl-add-ws -id:2 -name:"ws filial 2"
jc repl-add-ws -id:3 -name:"ws filial 3"
...
� �.�.


�����������:

jc repl-snapshot
jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



��������� ������� �������
---------------------------------


�����������:

jc repl-info
jc repl-create -id:XXX


�����������:


jc repl-snapshot
jc repl-sync -dir:f:\jdtx-repl\ -mark:true



������
---------------------------------


-Xms256m -Xmx1024m