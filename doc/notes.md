## Разные мысли, в т.ч. прошлых лет


### Фильтрация данных

Фильтры — расширяемые (добавление новых фильтров реализовано через интерфейс
"Вытащить из базы очередную порцию данных для сервера/для рабочей станции".

Фильтры «вниз» (от сервера на рабочую станцию) и «наверх» могут быть разными,
например от рабочей станции принимать оплаты за все услуги, а на рабочую станцию спускать только оплаты за газ.

Изменение/добавление/удаление фильтра происходит в двух режимах:
приводить данные на рабочей станции в соответствие новому фильтру (синхронный) или нет, т.е.
из БД станции будут удалены все данные, соответствующие старому варианту фильтра и будут добавлены все данные,
соответствующие новому фильтру. Нюанс: если фильтр выполнен по типу: все записи в таблице A, id которых находится в таблице B.
Как реализовать синхронный режим при изменении в таблице B?
События «включение/выключение фильтра» в данном случае не происходит, требуется при каждом изменении таблицы B.

KS: Глубокий анализ структуры и связанных с фильтром данных на предмет соответствия фильтрам.
Долго конечно работать будет, но вариантов не вижу.

Например: разрешаем отправлять на рабочую станцию данные клиентов.
Тогда на рабочую станцию не только начнут попадать новые клиенты,
но и будут сразу переданы все ранее введенные клиенты.

Предустановленные фильтры — горизонтальные и вертикальные срезы, в виде параметризированных SQL-запросов.
Параметры устанавливаются для каждой рабочей станции.
(А как насчет того, что фильтры — это не только ограничение данных, но и способ их преобразования и конвертации?
KS: И получится сервер объектов :))

Срочные запросы данных с сервера, высшим приоритетом.
Поступает Астанинский больной в Алма-ату — дайте инфу из Астаны.
Имеющаяся инфа поступает, а потом поступает срочно внесенная.
При редактировании записи можно не отправлять данные на сервер,
если нет реальных изменений в полях (или изменения затронули отфильтрованные поля).



### Группы связных полей

Ситуация, когда на двух узлах одновременно изменили записи в таблице А, но в разных полях требует двух режимов обновления:

  * обновление целыми записями (применяется, если поля зависимы друг от друга, например «число» и «месяц рождения», которые скорее всего правятся вместе). В этом случае в окончательном варианте записи будет отражено только последнее исправление.

  * обновление только измененных полей (если поля независимы друг от друга, например «Фамилия» и «Имя»). В таком случае в окончательном варианте записи будут отражены оба исправления.

Развитием идеи будет универсальное решение: по умолчанию все записи изменяются «вся запись целиком», однако можно включить режим «раздельно по полям», в рамках этого режима можно указать группы полей, изменяющиеся вместе, в таком случае изменение одного поля группы будет приводить к модификации остальных.

Например, в таблице имеются поля: «Фамилия», «Имя», «Город проживания», «Улица проживания». Поля «Город проживания» и «Улица проживания» включаем в группу, тогда исправление «Фамилия» может быть выполнено на одной машине, а исправление «Имя» - на другой и оба изменения будут сохранены, а ситуация указания улицы, не существующей в городе будет исключена (такая ситуация могла бы возникнуть, если бы на одной машине изменили город и улицу, а одновременно с этим на другой — только улицу и эта правка оказалась бы последней).


###### Пример:

Исходная запись:

^  Код\\ (Id)  ^  Наименование\\ (Name)  ^  Оптовая цена\\ (MinPrice)  ^  Розничная цена\\ (MaxPrice)  ^
|  1234  |  Сахар песок  |  140  |  148  |

Изменения на узле 1 (показано синим):

^  Код\\ (Id)  ^  Наименование\\ (Name)  ^  Оптовая цена\\ (MinPrice)  ^  Розничная цена\\ (MaxPrice)  ^
|  1234  |  Сахар песок  |  <html><font bold=1 color=navy>145</font></html>  |  148  |

Изменения на узле 2 (показано красным):

^  Код\\ (Id)  ^  Наименование\\ (Name)  ^  Оптовая цена\\ (MinPrice)  ^  Розничная цена\\ (MaxPrice)  ^
|  1234  |  Сахар песок  |  140  |  <html><font color=red>142</font></html>  |

Обмен узла 1 с сервером. Полученный результат:

^  Код\\ (Id)  ^  Наименование\\ (Name)  ^  Оптовая цена\\ (MinPrice)  ^  Розничная цена\\ (MaxPrice)  ^
|  1234  |  Сахар песок  |  <html><font color=navy>145</font></html><sup>(от узла 1)</sup>  |  148  |

Теперь выполним обмен узла 2 с сервером. В зависимости от настройки группировки полей итоговый результат выглядит по-разному:

**Вариант 1**: Независимые поля «Оптовая цена» и «Розничная цена»

^  Код\\ (Id)  ^  Наименование\\ (Name)  ^  Оптовая цена\\ (MinPrice)  ^  Розничная цена\\ (MaxPrice)  ^
|  1234  |  Сахар песок  |  <html><font color=navy>145</font></html><sup>(от узла 1)</sup>  |  <html><font color=red>142</font></html><sup>(от узла 2)</sup>  |

Наблюдаем нарушение логической целостности: оптовая цена больше розничной цены, зато изменения от каждого узда не потеряны.

**Вариант 2**: Сгруппированные поля «Оптовая цена» и "Розничная цена"

^  Код\\ (Id)  ^  Наименование\\ (Name)  ^  Оптовая цена\\ (MinPrice)  ^  Розничная цена\\ (MaxPrice)  ^
|  1234  |  Сахар песок  |  <html><font color=red>140</font></html><sup>(от узла 2)</sup>  |  <html><font color=red>142</font></html><sup>(от узла 2)</sup>  |

Наблюдаем сохранение логической целостности: оптовая цена меньше розничной цены, однако изменения от узла 1 потеряны.



### Поддержка смены структуры базы (ранние мысли)

Смена структуры базы данных выполняется без перенастройки и перезапуска системы. Смена структуры осуществляется только с центрального сервера в двух режимах по выбору. KS: Мысль конечно здравая, но сколько нам крови в со выпили, требуя смену структуры на репликах отдельно. Например: оччень болшие изменения, скриптами, сложные. Они приводят к модификации всех записей — очень большой размер реплик. Быстрее сделать вне репликации. Делали exe для сложных изменений. Подхват незаконченного при отключении света.
* Ручная. Администратор явно начинает смену структуры, указывает скрипт (скрипты) на смену структуры (а как же разные СУБД? KS: Есть механизмы (на java точно либы есть такие), которые вводят некий универсальный язык манипулирования структурой и драйвера для разных субд. Но конечно триггера и хранимые - ручками), заканчивает смену структуры. [Как в реплюсе]
* Полуавтоматическая. При обнаружении изменения структуры (когда и как? - например, контролировать изменения структуры удобно при формировании очередной порции реплик) система прочитывает структуру БД и сравнивает ее со старой. Нужные изменения оформляются в виде DDL-команд и помещаются в общую очередь реплик. При получении реплики на смену структуры рабочая станция меняет свою структуру, и новые изменения шлет в новом формате. KS: Автоматом не выйдет(нужен человек). Вспомни со и проблему «поле переименовали»



### Проблемы распределеннных баз данных

Помимо очевидных преимуществ, работа с распределенными базами данных приносит и проблемы.

* Дублирование значений в справочниках - требуется уход за ними, своевременно "схлопывание" дублей
* Логичекое несоответствие (попытки продать 1 товар двум покупателям)
* ВременнЫе задержки

Следует особо подчеркнуть, что эти сложности не являются частными проблемами именно репликации или его реализации,
а проистекают именно из самого факта работы в распределенной среде.



### Ограничения на прикладное ПО, не упомянутые ранее

* Данные, прошедшие проверку (удовлетворяющие требованиям бизнес-логики) прикладного ПО на рабочей станции,
  должны гарантированно проходить ее и на сервере.
  Следствие — камеральный контроль (например — проверки на уникальность значения) могут плохо кончится,
  если их проверка реализована на уровне БД. Последнее категорически не рекомендуется.
  При включении таблицу в репликацию система производит анализ таких ограничений и выдает предупреждение
  с возможностью удаления таких ограничений или замены ограничения типа  unique (Name) на unique (Name + Код рабочей станции)

* Возможны ситуации нарушения логической целостности.

  Например: на первой рабочей станции: «оплата клиент1», репликация.

  На второй станции: репликация, обнаружение «оплата клиент1», выполнение «продажа клиент1».

  На первой рабочей станции: исправление ошибочно введенной «оплата клиент1» на «оплата клиент2», репликация.
  Без специальных мер «продажа клиент1» останется без изменений, а должна быть исправлена на «продажа клиент2».

* Обязательно наличие в реплицируемых таблицах первичного ключа.



### Разведение первичных ключей

##### Суть проблемы
Проблема пересечения первичных ключей возникает, когда разные узлы репликационной сети добавляют записи в одну и ту же таблицу.
Представим, что три разных магазина пополняют единый классификатор товаров:

```
Узел 1:
^id^Наименование^
|1000|Сахар песок|
```

```
Узел 2:
^id^Наименование^
|1000|Сигареты "Полет"|
```

```
Узел 3:
^id^Наименование^
|1000|Тетрадь общая (48 л.)|
```

Какая запись в итоге попадет на сервер, если поле id уникально (первичный ключ)? Какие записи окажутся на рабочих станциях, если нужно, чтобы классификатор товаров был единым? Такая проблема не возникает в локальной сети и характерна для любой системы репликации.

##### Варианты решения

* Автоматическое управление с диапазонами для каждой станции. Диапазоны ведутся на сервере и выделяются каждой рабочей станции по мере необходимости. Требует поддержки от прикладного ПО (а. использовать для генерации первичного ключа определенную хранимую процедуру; б. добавить триггера before insert, в которых задавать id правильным значением, если оно не заполнено (или даже заполненное  переписывать принудительно). Для MS SQL помимо добавления триггера следует преобразовать все identity поля в обычные и попытаться решить проблему, что пользователь берет @@identity чтобы узнать, с каким id добавилась запись). Преимущества — нет изменения в структуре пользовательских таблиц, недостатки — выставляются требования к разработке прикладного ПО. KS: Это должно быть опционально
* На всех рабочих станциях диапазоны первичного ключа одинаковое. Автоматическое управление с перекодировкой первичных ключей (в каждую таблицу добавляется суррогатный ключ?? А может просто таблица перекодировки на сервере?). Преимущества — нет особых требований к разработке прикладного ПО, недостатки — усложнение идентификации записей, нужны дополнения в структуру пользовательских таблиц (это если добавляется суррогатный ключ).
* Перекодировка на лету. Во время сеанса репликации сервер передает рабочей станции новые значения первичного ключа. Ключи уникальны по всей сети. Недостатки: требуется знание структуры ссылок, например, через foreign key.
* Использование в качестве первичного ключа GUID. В составе программного пакета Jadatex имеются средства для преобразования обычных первичных ключей в GUID.
* Использование составных первичных ключей.
* Уникальность первичного ключа гарантируется другим способом, без помощи системы репликации.


