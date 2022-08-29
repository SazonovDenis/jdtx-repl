##### Разведение первичных ключей
Проблема пересечения [[info#primary_key|первичных ключей]] возникает, когда разные [[info#primary_key|узлы]] [[info#primary_key|репликационной сети]] добавляют записи в одну и ту же таблицу. Представим, что три разных магазина пополняют единый классификатор товаров:

Узел 1:
^id^Наименование^
|1000|Сахар песок|

Узел 2:
^id^Наименование^
|1000|Сигареты "Полет"|

Узел 3:
^id^Наименование^
|1000|Тетрадь общая (48 л.)|

Какая запись в итоге попадет на сервер, если поле id уникально (первичный ключ)? Какие записи окажутся на рабочих станциях, если нужно, чтобы классификатор товаров был единым? Такая проблема не возникает в локальной сети и характерна для любой системы репликации. Jadatex предлагает несколько вариантов решения проблемы:
  * Автоматическое управление с диапазонами для каждой станции. Диапазоны ведутся на сервере и выделяются каждой рабочей станции по мере необходимости. Требует поддержки от прикладного ПО (а. использовать для генерации первичного ключа определенную хранимую процедуру; б. добавить триггера before insert, в которых задавать id правильным значением, если оно не заполнено (или даже заполненное  переписывать принудительно). Для MS SQL помимо добавления триггера следует преобразовать все identity поля в обычные и попытаться решить проблему, что пользователь берет @@identity чтобы узнать, с каким id добавилась запись). Преимущества — нет изменения в структуре пользовательских таблиц, недостатки — выставляются требования к разработке прикладного ПО. KS: Это должно быть опционально
  * На всех рабочих станциях диапазоны первичного ключа одинаковое. Автоматическое управление с перекодировкой первичных ключей (в каждую таблицу добавляется суррогатный ключ?? А может просто таблица перекодировки на сервере?). Преимущества — нет особых требований к разработке прикладного ПО, недостатки — усложнение идентификации записей, нужны дополнения в структуру пользовательских таблиц (это если добавляется суррогатный ключ).
  * Перекодировка на лету. Во время сеанса репликации сервер передает рабочей станции новые значения первичного ключа. Ключи уникальны по всей сети. Недостатки: требуется знание структуры ссылок, например, через foreign key.
  * Использование в качестве первичного ключа GUID. В составе программного пакета Jadatex имеются средства для преобразования обычных первичных ключей в GUID.
  * Использование составных первичных ключей.
  * Уникальность первичного ключа гарантируется другим способом, без помощи системы репликации.