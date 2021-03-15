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
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class MsFactoryTest
    {
        private static Random random;
        [ClassInitialize]
        public static void InitDatabase(TestContext contex)
        {
            random = new Random();
            try
            {
                MsQueryFactory.Instance.ConfigureConnection(new MsConnection("Prometheus.ca.sbrf.ru\\Prometheus", "", "", "", 30, useWinAuth: true).ToConnectionString(), "Prometheus");

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
        public void TestFactoryConnectionsLimit()
        {
            MsQueryFactory.Instance.transactionLimithPerConnection = 2;
            MsQueryFactory.Instance.GetConnectionRow(null).GetConnection(); //инициализация списка соединений
            int conCntBefore = MsQueryFactory.Instance.connectionsList.Count();
            Debug.WriteLine($"conCntBefore={conCntBefore}");
            int cntThread = 5;
            int cntThreadFinished = 0;
            int cntThreadStarted = 0;
            for (int i = 0; i < cntThread; i++)
            {
                int j = i;
                Task.Run(() =>
                {
                    Debug.WriteLine($"query #{j} created");
                    Interlocked.Increment(ref cntThreadStarted);
                    MsQueryFactory.NewQuery($"waitfor delay '00:00:05' select 1 --{j}").Execute();
                    Interlocked.Increment(ref cntThreadFinished);
                    Debug.WriteLine($"query #{j} finished");
                });
            }           
            while(cntThreadStarted!=cntThread)
            {
                Thread.Sleep(100);
            }
            Debug.WriteLine("all query threads started");
            int conCntAfter = MsQueryFactory.Instance.connectionsList.Count();
            for (int i=0;i< conCntAfter; i++)
            {
                Debug.WriteLine("connection #" + i.ToString());
                foreach(var query in MsQueryFactory.Instance.connectionsList[i].queryList)
                {
                    Debug.WriteLine("\t\tquery: " + query.ToString());
                }
            }
            Assert.AreEqual(3, conCntAfter, "Должно быть создано 3 соединения");
            while (cntThreadFinished != cntThread)
            {
                Thread.Sleep(100);
            }
            //Thread.Sleep(500);
            Debug.WriteLine("all query threads finished");
            //после того, как все потоки завершились вызов очередной newQuery должен убрать лишние коннекшины
            conCntAfter = MsQueryFactory.Instance.connectionsList.Count();
            for (int i = 0; i < conCntAfter; i++)
            {
                Debug.WriteLine("connection #" + i.ToString());
                foreach (var query in MsQueryFactory.Instance.connectionsList[i].queryList)
                {
                    Debug.WriteLine("\t\tquery: " + query.ToString());
                }
            }
            Assert.AreEqual(1, conCntAfter, "после того, как все потоки завершились вызов очередной newQuery должно остаться одно соединение");
            MsQueryFactory.Instance.transactionLimithPerConnection = 10;
        }

        [TestMethod]
        public void TestNewFactoryInstance()
        {
            MsQueryFactory hyperion = MsQueryFactory.NewFactory(new MsConnection("hyperion.ca.sbrf.ru\\hyperion", "TB70_SANDBOX", "", "", useWinAuth : true).ToConnectionString(), "hyperion");
            MsQuery q = hyperion.NewQuery("select count(*) from sys.databases where name = 'TB70_SANDBOX'");
            Assert.AreEqual(1, q.GetFirstCell<int>(), "На сервере Hyperion ожидалось найти базу данных TB70_SANDBOX");
        }

    }
}
