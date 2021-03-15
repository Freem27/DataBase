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


# withForJson
обработка запросов MsSql возвращающих занчения в формате json

# Query Builder
Класс для построения запросов

# QueryList 
Класс для выполнения нескольких запросов в рамках одной транзакции
