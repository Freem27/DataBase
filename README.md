# DataBase
Библиотека для работы с БД MSSQL


## Подключение и настройки 
Для работы необходимо установить библиотеку Newtonsoft.Json

```c#
using TDV.DataBase;
using Newtonsoft.Json.Linq;
```
сконфигурировать подключение по умлочанию достаточно 1 раз. Класс MsQueryFactory пердставляет собой синглтон фабрику для работы с одним сервером БД

```c#
--подключение с логином и паролем
MsQueryFactory.Instance.ConfigureConnection(new MsConnection("localhost", "ttk", "sa", "master").ToConnectionString(), "connectionAlias");

--подключение с учеткой windows
MsQueryFactory.Instance.ConfigureConnection(new MsConnection("localhost", "ttk", "", "",useWinAuth:true).ToConnectionString(), "connectionAlias");
```

если требуется работать с несколькими БД можно создать еще одну фабрику
```c#
MsQueryFactory factory2 = MsQueryFactory.NewFactory(new MsConnection("localhost", "database2", "", "", useWinAuth: true).ToConnectionString(), "db2");
```

MsQueryFactory выполняет по 10 запросов в рамках одного подключения. Если в момент добавления нового запроса в рамках подключения уже выполняется 10 будет создано еще одно подключение с таким же лимитом запросов. Количество создаваемых подключений не ограничего. После выполнения запросов производится чистка. Не используемые подключения закрываются.
Можно изменить лимит запросов на одно соединение с БД:
```c#
MsQueryFactory.Instance.transactionLimithPerConnection = 20;
```

## Получение данных (select\exec для табличных функций)

```c#
MsQuery q = MsQueryFactory.NewQuery("SELECT TOP 2 database_id,name,create_date from sys.databases");

DataTable table= q.ToDataTable();
JArray resultRows = q.ToJArray();     //вернет все строки в JArray
JObject resultOneRow = q.ToJObject(); //вернет первую строку запроса в JObject 
int cnt=q.GetFirstCell<int>();        //получениее первой ячейки запроса
```
*resultRows*
```json
[
  {
    "database_id": 1,
    "name": "master",
    "create_date": "2003-04-08T09:13:36.39"
  },
  {
    "database_id": 2,
    "name": "tempdb",
    "create_date": "2017-02-17T18:34:20.773"
  }
]
```

*resultOneRow*
```json
{
  "database_id": 1,
  "name": "master",
  "create_date": "2003-04-08T09:13:36.39"
}
```

## Выполнение запросов (ddl\insert\update\delete\exec)
### Вставка данных
```c#
//создание таблицы
MsQuery q = MsQueryFactory.NewQuery("create table ##temp( id int primary key identity(1,1))");
q.Execute();
MsQueryFactory.NewQuery("alter table ##temp add text varchar(100)").Execute();

//запросы можно писать в одну строку
//Обычная вставка
MsQueryFactory.NewQuery("insert into ##temp (text) values ('test')").Execute();

//получение id вставленной строки
int newId = MsQueryFactory.NewQuery("insert into ##temp (text) output inserted.id values ('test2')").InsertSqlReturnId<int>();
```

**bulk insert**
 ```c#
//инициализация таблицы для вставки
DataTable tableForInsert = new DataTable();
tableForInsert.Columns.Add(new DataColumn("text"));
//можно так же запросить структуру таблицы из бд
//tableForInsert = MsQueryFactory.NewQuery("select top 0 * from dbo.testTable").ToDataTable();

tableForInsert.TableName = "dbo.testTable";

for(int i=0;i<4;i++)
{
    DataRow newRow = tableForInsert.NewRow();
    newRow["text"] = i.ToString();
    tableForInsert.Rows.Add(i);
}

MsQuery q = MsQueryFactory.NewQuery("").BulkInsert(tableForInsert);
//проверка на ошибки
if (q.withError)
    Console.WriteLine(q.error);
```

## update\delete 
аналогично вставке
```c#
MsQueryFactory.NewQuery("update ##temp set text='ddd' where id<3").Execute();
MsQueryFactory.NewQuery("delete from ##temp  where id>3").Execute();
```

# Параметры запроса
```c#
MsQuery q = MsQueryFactory.NewQuery("insert into #table (id,text,datetimeCol) values (@id,@column2,@column3)");
q.AddParameter("@id", 1);
q.AddParameter("@column2", "text");
q.AddParameter("@column3", DateTime.Now);
if (q.Execute().withError)
Console.WriteLine(q.error);
```
этот же запрос можно написать в 1 строку
```c#
MsQuery q = MsQueryFactory.NewQuery("insert into #table (id,text,datetimeCol) values (@id,@column2,@column3)").AddParameter("@id", 1).AddParameter("@column2", "text").AddParameter("@column3", DateTime.Now).Execute();
if (q.withError)
    Console.WriteLine(q.error);
```

реализована возможность биндить в запрос списки
```c#
List<int> idList = new List<int>() { 1, 2, 3 };
MsQuery q = MsQueryFactory.NewQuery("select * from sys.databses where database_id in (@list)").AddParametersList<int>("@list", idList);
```

# withForJson
MsSql может возвращать результаты запроса в формате JSON. Чтобы получать результат в виде JObject\JArray реализован класс, возвращающий занчения в формате json
```c#
var q = MsQueryFactory.NewQuery("  SELECT database_id,create_date FROM sys.databases  for  JSON PATH").WithForJson();
JArray result = q.ToJArray();
JObject resultFirstRow = q.ToJObject();

//класс WithForJson аналогично классу Query обладает свойствами withError (bool) и error (string)
if(q.withError)
    throw new Exception(q.error)
```

# Query Builder
Класс для построения update\insert запросов.
Вставляемые\изменяемые поля добавляются с помощью метода AddUpsertParametr, который обязательно принимает в себя название колонки в БД (name) и значение

# QueryList 
Класс для выполнения нескольких запросов в рамках одной транзакции
```c#
QueryBuilder qb = new QueryBuilder();
qb.SetMode(QueryBuilder.BuilderType.insert); //выбираем тип запроса update\insert
qb.SetTableName("#temp");
qb.AddUpsertParametr("id", new Random().Next()); //добавляем параметр для изменения колонки id
DateTime oldDate = DateTime.Now.AddDays(-1);
DateTime newDate = DateTime.Now;
qb.AddUpsertParametr("date", newDate, oldDate); //парамерт будет добавлен в запрос только если newDate!=oldDate
qb.SetWhereSqlPart("where count in (@Cnt)").AddParametr("@Cnt", new List<int>() { 1,2,3});//добавляем условие where
qb.AddReturningValue("id"); // возвращаемое значение. доступно только для insert запросов (обертка для inserted.[название колонки])

if(qb.HasUpsertParametrs()) //если нет параметров для обновления\вставки нет смысла выполнять запрос
{
    MsQuery q = qb.ToMsQuery();
    if (q.Execute().withError)
        throw new Exception(q.error);
}
```

последовательность вызова методов не имеет значения. sql запрос формируется на этапе вызова метода ToMsQuery()
этот же запрос можно написать в одну строку:
```c#
QueryBuilder qb = new QueryBuilder(QueryBuilder.BuilderType.insert).SetTableName("#temp").AddUpsertParametr("id", new Random().Next()).AddUpsertParametr("date", newDate, oldDate).SetWhereSqlPart("where count in (@Cnt)").AddParametr("@Cnt", new List<int>() { 1,2,3}).AddReturningValue("id");
```

# QueryList 
Класс для выполнения нескольких запросов в рамках одной транзакции
существует 2 режима: ручной коммит (метод) и авто (при вызове метода Execute())

авто:
```c#
MsQueryList ql = MsQueryFactory.NewQueryList(continueIfError: false, manualTransaction: false);
ql.AddNewQuery("insert into #temp (id) values (1)");
ql.AddNewQuery("update #temp set id=1 where id=@id)").AddParameter("@id",32);
ql.Execute();
if (ql.withError)
    throw new Exception(ql.GetErrorSplitString());
```

ручной режим (После вызова метода Execute() транзакция не закрывается):
```c#
using (MsQueryList ql = MsQueryFactory.NewQueryList(manualTransaction: true))
{
    ql.AddNewQuery("create table #temp ( id int)");
    ql.Execute();
    ql.AddNewQuery("insert into #temp (id) values (1)");
    ql.AddNewQuery("insert into #temp (id) values (2)");
    ql.AddNewQuery("insert into #temp (id) values (3)");
    ql.AddNewQuery("insert into #temp (id) values (4)");
    ql.Execute();
    ql.AddNewQuery("delete from #temp where id=@id").AddParameter("@id", 3);
    ql.Execute();
    MsQuery q = ql.AddNewQuery("select count(*) from #temp");
    int cnt=q.GetFirstCell<int>();
    Console.WriteLine(cnt); //3
    //ql.AddQuery(MsQueryFactory.NewQuery("exec dbo.proc1")); //допустимо добавление запросов, созданные вне MsQueryList
    ql.ManualCommit();

    if (ql.withError)
        throw new Exception(ql.GetErrorSplitString());
}
```

Если не вызывать метод ManualCommit() все изменения в БД будут отменены
Вы можете отменить сделанные измения вызовом метода ManualRollback() (Только в ручном режиме до вызова ManualCommit())
