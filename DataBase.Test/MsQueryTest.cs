using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TDV.DataBase;
using System.Linq;
using System.Data;
using Newtonsoft.Json.Linq;

namespace DataBase.Test
{
    [TestClass]
    public class MsQueryTest
    {
        private static string tableName = "[SQL_TB70_SANDBOX].tdv.unitTest";
        private static string bulkTableName = "[SQL_TB70_SANDBOX].tdv.bulkTest";
        private static Random random;
        [ClassInitialize]
        public static void InitDatabase(TestContext contex)
        {
            random = new Random();
            try { 
                MsQueryFactory.Instance.ConfigureConnection(new MsConnection("Prometheus.ca.sbrf.ru\\Prometheus", "", "", "", 30, useWinAuth: true).ToConnectionString(), "Prometheus");
                MsQuery q = MsQueryFactory.NewQuery($"drop table if exists {tableName}");
                if (q.Execute().withError)
                    Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

                q = MsQueryFactory.NewQuery($@"create table {tableName}(
	                    id int primary key identity,
	                    name varchar(10)
                    )");
                if (q.Execute().withError)
                    Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");
                //заполнение таблицы
                for(int i=0;i<10;i++)
                {
                    q = MsQueryFactory.NewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(i+1));
                    if(q.Execute().withError)
                        Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");
                }
            }catch(Exception e)
            {
                Assert.Fail($"Ошибка InitDatabase(): {e.Message} ");
            }
        }

        private static string RandomString(int len)
        {
            string chars = "ЙЦУКЕНГШЩЗФЫВАПРОЛДЖЭХЪЯЧСМИТЬБЮ123456789";
            return new string(Enumerable.Repeat(chars, len).Select(s => s[random.Next(s.Length)]).ToArray());
        }

        [TestMethod]
        public void TestInsert()
        {
            string testName = "testInsert";
            MsQuery q = MsQueryFactory.NewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", testName);
            if(q.Execute().withError)
                Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

            q = MsQueryFactory.NewQuery($"select name from {tableName} where name=@name").AddParameter("@name", testName);
            string result = q.GetFirstCell<string>();


            Assert.AreEqual(testName, result);
        }

        [TestMethod]
        public void TestInsertReturnId()
        {
            MsQuery q1 = MsQueryFactory.NewQuery($"insert into {tableName} (name) output inserted.id values(@name)").AddParameter("@name",RandomString(5));
            int id1 =q1.InsertSqlReturnId<int>();
            if(q1.withError)
                Assert.Fail($"Ошибка при выполнении команды {q1.ToString()}: {q1.error} ");

            MsQuery q2 = MsQueryFactory.NewQuery($"insert into {tableName} (name) output inserted.id values(@name)").AddParameter("@name", RandomString(6));
            int id2 = q2.InsertSqlReturnId<int>();
            if (q2.withError)
                Assert.Fail($"Ошибка при выполнении команды {q2.ToString()}: {q2.error} ");
            Assert.AreEqual(1, id2 - id1);
        }

        [TestMethod]
        public void TestGetFirstCell()
        {
            MsQuery q = MsQueryFactory.NewQuery($"select count(*) cnt from {tableName}");
            int count = q.GetFirstCell<int>();
            Assert.IsTrue(count > 9, $"Количество строк в таблице {tableName} должно быть больше 9");
        }

        [TestMethod]
        public void TestToJArray()
        {
            MsQuery q = MsQueryFactory.NewQuery($"select id,name from {tableName}");
            JArray result = q.ToJArray();
            Assert.IsTrue(result.Count() > 9, "Количество строк ожидалось больше 9");
            JObject firstRow = (JObject)result.First();
            Assert.IsTrue(firstRow.ContainsKey("id") && firstRow.ContainsKey("name"), "Объекты JArray не содрежат ключи id и name");
            Assert.IsTrue(firstRow.Value<int>("id") == 1, "id первой строки таблицы должен быть равен 1");
        }

        [TestMethod]
        public void TestToJObject()
        {
            MsQuery q = MsQueryFactory.NewQuery($"select count(*) cnt from {tableName}");
            JObject result = q.ToJObject();
            Assert.IsTrue(result.ContainsKey("cnt"), "Результат должен содержать ключ cnt");
            Assert.IsTrue(result.Value<int>("cnt") > 9, $"Количество строк в таблице {tableName} должно быть больше 9");
        }


        [TestMethod]
        public void TestToDatatable()
        {
            MsQuery q = MsQueryFactory.NewQuery($"select count(*) cnt from {tableName}");
            DataTable result = q.ToDataTable();
            Assert.IsTrue(result.Columns.Contains("cnt"), "Результат должен содержать колонку cnt");
            Assert.IsTrue(Int32.Parse(result.Rows[0]["cnt"].ToString()) > 9, $"Количество строк в таблице {tableName} должно быть больше 9");
        }


        [TestMethod]
        public void TestWithForJsonTest()
        {
            MsQuery q = MsQueryFactory.NewQuery($"select id,name from {tableName}  FOR JSON PATH,INCLUDE_NULL_VALUES");
            JArray result = q.WithForJson().ToJArray();
            Assert.IsTrue(result.Count() > 9, "Количество строк ожидалось больше 9");
            JObject firstRow = (JObject)result.First();
            Assert.IsTrue(firstRow.ContainsKey("id") && firstRow.ContainsKey("name"), "Объекты JArray не содрежат ключи id и name");
            Assert.IsTrue(firstRow.Value<int>("id") == 1, "id первой строки таблицы должен быть равен 1");
            q = MsQueryFactory.NewQuery($"select count(*) cnt from {tableName}  FOR JSON PATH,INCLUDE_NULL_VALUES ,without_array_wrapper");
            JObject resultJObject = q.WithForJson().ToJObject();
            Assert.IsTrue(resultJObject.ContainsKey("cnt"), "Результат должен содержать ключ cnt");
            Assert.IsTrue(resultJObject.Value<int>("cnt") > 9, $"Количество строк в таблице {tableName} должно быть больше 9");
        }

        [TestMethod]
        public void TestBulkInsert()
        {
            MsQuery q = MsQueryFactory.NewQuery($"drop table if exists {bulkTableName}");
            if (q.Execute().withError)
                Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

            q = MsQueryFactory.NewQuery($@"create table {bulkTableName}(
	                    id int,
	                    name varchar(10)
                    )");

            if (q.Execute().withError)
                Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

            DataTable forInsert = MsQueryFactory.NewQuery($"select * from {bulkTableName}").ToDataTable();
            for (int i =1;i<=1000;i++)
            {
                DataRow newRow = forInsert.NewRow();
                newRow["id"] = i;
                newRow["name"] = RandomString(10);
                forInsert.Rows.Add(newRow);
            }

            q = MsQueryFactory.NewQuery("").BulkInsert(bulkTableName,forInsert);
            if(q.withError)
                Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

            int rowsCnt = MsQueryFactory.NewQuery($"select count(*) cnt from {bulkTableName}").GetFirstCell<int>();
            Assert.AreEqual(1000, rowsCnt, $"В таблицу {bulkTableName} должно было вставится 1000 строк");

            forInsert.TableName = bulkTableName;
            q = MsQueryFactory.NewQuery("").BulkInsert(forInsert);
            if (q.withError)
                Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

            int rowsCnt2 = MsQueryFactory.NewQuery($"select count(*) cnt from {bulkTableName}").GetFirstCell<int>();
            Assert.AreEqual(2000, rowsCnt2, $"В таблицу {bulkTableName} должно было вставится 2000 строк");
        }
    }
}
