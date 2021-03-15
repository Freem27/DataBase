using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TDV.DataBase;
using System.Linq;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;

namespace DataBase.Test
{
    /// <summary>
    /// Summary description for MsQueryListTest
    /// </summary>
    [TestClass]
    public class MsQueryListTest
    {
        private static string tableName = "[SQL_TB70_SANDBOX].tdv.unitTest";
        private static Random random;
        public MsQueryListTest()
        {
            try
            {
                random = new Random();
                if (string.IsNullOrEmpty(MsQueryFactory.Instance.connectionString) || MsQueryFactory.Instance.factoryName!= "Prometheus")
                    MsQueryFactory.Instance.ConfigureConnection(new MsConnection("Prometheus.ca.sbrf.ru\\Prometheus", "", "", "", 30, useWinAuth: true).ToConnectionString(), "Prometheus");
                MsQuery q = MsQueryFactory.NewQuery($"drop table if exists {tableName}");
                if (q.Execute().withError)
                    Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");

                q = MsQueryFactory.NewQuery($@"create table {tableName}(
	                    id int primary key identity,
	                    name varchar(10)
                    )");
                if(q.Execute().withError)
                    Assert.Fail($"Ошибка при выполнении команды {q.ToString()}: {q.error} ");
            }
            catch (Exception e)
            {
                Assert.Fail($"Ошибка InitDatabase(): {e.Message} ");
            }
        }
        private static string RandomString(int len)
        {
            string chars = "ЙЦУКЕНГШЩЗФЫВАПРОЛДЖЭХЪЯЧСМИТЬБЮ123456789";
            return new string(Enumerable.Repeat(chars, len).Select(s => s[random.Next(s.Length)]).ToArray());
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod]
        public void TesQueryListMultiplyInsert()
        {
            MsQueryList ql = MsQueryFactory.NewQueryList();
            ql.AddNewQuery($"truncate table {tableName}");
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            if (ql.Execute().withError)
                Assert.Fail(ql.GetErrorSplitString());

            MsQuery q = MsQueryFactory.NewQuery($"select count(*) from {tableName}");
            int countRows = q.GetFirstCell<int>();

            Assert.AreEqual(2, countRows, "В таблицу должно было вставиться 2 строки");
            MsQueryFactory.NewQuery($"truncate table {tableName}").Execute();

            Assert.AreEqual(1, MsQueryFactory.Instance.connectionsList.Count(), "Должно быть 1 активное соединение");
        }



        [TestMethod]
        public void TestContinueIfError()
        {
            MsQueryFactory.NewQuery($"truncate table {tableName}").Execute(); //очистка таблицы
            MsQueryList ql = MsQueryFactory.NewQueryList();
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(20)); //ошибка т.к. размер поля 10 символов
            if (!ql.Execute().withError)
                Assert.Fail("Некорректрый sql запрос. Должно быть сообщение об ошике");

            MsQuery q = MsQueryFactory.NewQuery($"select count(*) from {tableName}");
            int countRows = q.GetFirstCell<int>();
            Assert.AreEqual(0, countRows, "В таблицу не должны были вставиться строки");


            ///continueIfError:true
            ql = MsQueryFactory.NewQueryList(continueIfError:true);
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(20)); //ошибка т.к. размер поля 10 символов
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            if (!ql.Execute().withError)
                Assert.Fail("Некорректрый sql запрос. Должно быть сообщение об ошике");
            q = MsQueryFactory.NewQuery($"select count(*) from {tableName}");
            countRows = q.GetFirstCell<int>();
            Assert.AreEqual(2, countRows, "В таблицу дожно быть 2 строки");
        }

        [TestMethod]
        public void TestManualRollback()
        {
            MsQueryFactory.NewQuery($"truncate table {tableName}").Execute(); //очистка таблицы
            //в таблице одна строка
            MsQueryList ql = MsQueryFactory.NewQueryList(manualTransaction:true);
            ql.AddNewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            ql.Execute();
            DataTable forInsert = MsQueryFactory.NewQuery($"select top 0 * from {tableName}").ToDataTable();
            for (int i = 1; i <= 1000; i++)
            {
                DataRow newRow = forInsert.NewRow();
                newRow["id"] = i;
                newRow["name"] = RandomString(10);
                forInsert.Rows.Add(newRow);
            }
            ql.AddNewQuery().BulkInsert(tableName,forInsert);
            int cntBeforeCommit=ql.AddNewQuery($"select count(*) from {tableName}").GetFirstCell<int>();

            Assert.AreEqual(1001, cntBeforeCommit, "В таблице должно быть 1001 строк");

            if (ql.Execute().withError)
                Assert.Fail(ql.GetErrorSplitString());
            ql.ManualRollback();

            MsQuery q = MsQueryFactory.NewQuery($"select count(*) from {tableName} (nolock)");
            Assert.AreEqual(0 ,q.GetFirstCell<int>(), "После Rollback в таблице не должно оставаться строк");
            MsQueryFactory.NewQuery($"truncate table {tableName}").Execute(); //очистка таблицы
        }


        [TestMethod]
        public void TestManualCommit()
        {
            MsQueryFactory.NewQuery($"truncate table {tableName}").Execute(); //очистка таблицы

            MsQueryFactory.NewQuery($"insert into {tableName} (name) values (@name)").AddParameter("@name", RandomString(10));
            //в таблице одна строка
            MsQueryList ql = MsQueryFactory.NewQueryList(manualTransaction: true);
            ql.AddNewQuery($"delete from {tableName}");
            ql.Execute();
            DataTable forInsert = MsQueryFactory.NewQuery($"select top 0 * from {tableName}").ToDataTable();
            for (int i = 1; i <= 1000; i++)
            {
                DataRow newRow = forInsert.NewRow();
                newRow["id"] = i;
                newRow["name"] = RandomString(10);
                forInsert.Rows.Add(newRow);
            }
            ql.AddNewQuery().BulkInsert(tableName, forInsert);

            if (ql.Execute().withError)
                Assert.Fail(ql.GetErrorSplitString());
            ql.ManualCommit();

            int cntBeforeCommit = MsQueryFactory.NewQuery($"select count(*) from {tableName}").GetFirstCell<int>();

            Assert.AreEqual(1000, cntBeforeCommit, "В таблице должно быть 1000 строк");
            MsQueryFactory.NewQuery($"truncate table {tableName}").Execute(); //очистка таблицы
        }

        [TestMethod]
        public void TestReleaseConnection()
        {
            MsQueryFactory.Instance.transactionLimithPerConnection = 1;
            int conCntBefore= MsQueryFactory.Instance.connectionsList.Count();
            //запросы без ошибок
            for(int i=1;i<10;i++)
            {
                MsQueryList ql = MsQueryFactory.NewQueryList();
                ql.AddNewQuery("select 1");
                ql.AddNewQuery("select 1");
                ql.Execute();
            }
            int conCntNow= MsQueryFactory.Instance.connectionsList.Count();
            Assert.AreEqual(conCntBefore, conCntNow, "Запросы без ошибок. Количество подключений не должно увеличиться");


            //запросы с ошибками
            conCntBefore = MsQueryFactory.Instance.connectionsList.Count();
            for (int i = 1; i < 10; i++)
            {
                MsQueryList ql = MsQueryFactory.NewQueryList();
                ql.AddNewQuery("select 1 from ddd");
                ql.AddNewQuery("select 1");
                ql.Execute();
                
            }
            conCntNow = MsQueryFactory.Instance.connectionsList.Count();
            Assert.AreEqual(conCntBefore, conCntNow, "Запросы с ошибкой. Количество подключений не должно увеличиться");

            conCntBefore = MsQueryFactory.Instance.connectionsList.Count();
            List<MsQueryList> list = new List<MsQueryList>();
            for (int i = 1; i < 10; i++)
            {
                MsQueryList ql = MsQueryFactory.NewQueryList(manualTransaction:true);
                list.Add(ql);
                ql.AddNewQuery("select 1 from ddd");
                ql.AddNewQuery("select 1");
                ql.Execute();

            }

            foreach (MsQueryList ql in list)
                ql.ManualCommit();
            conCntNow = MsQueryFactory.Instance.connectionsList.Count();
            Assert.AreEqual(conCntBefore, conCntNow, "Запросы с ошибкой manualCommit. Количество подключений не должно увеличиться");
        }
    }
}
