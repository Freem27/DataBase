using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Web.Configuration;
using System.Data;
using System.Diagnostics;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Data.Common;
using TDV.DataBase.Extentions;

namespace TDV.DataBase
{

    public abstract class QueryFactoryBase : IDisposable
    {
        public class ConnectionRow 
        {
            private QueryFactoryBase factory;
            private object isLock;
            
            public ConnectionRow(QueryFactoryBase factory, object isLock, DbConnection connection)
            {
                this.isLock = isLock;
                this.factory = factory;
                this._connection = connection;
                dtCreated = DateTime.Now;
                queryList = new List<object>();
            }
            private DbConnection _connection;
            public DbConnection GetConnection()
            {
                return factory.TryInitConnection(_connection);
            }
            public DateTime dtCreated;
            public List<object> queryList;

            /// <summary>
            /// удаляет ссылку на query и списка queryList
            /// </summary>
            /// <param name="query"></param>
            /// <returns></returns>
            public ConnectionRow ReleaseConnection(object query)
            {
                lock(isLock)
                { 
                    queryList.Remove(query);

                    //закрыть лишние соединения (у которых нет транзакция)
                    var cns = factory.connectionsList.Where(x => x.queryList.Count() == 0);
                    while (cns.Count() > 1)
                    {
                        var forClose = cns.First();
                        forClose.GetConnection().Close();
                        factory.connectionsList.Remove(forClose);
                    }
                    return this;
                }
            }
        }
        public List<ConnectionRow> connectionsList;
        public string factoryName;
        public string connectionString;
        private object isLock=new object();
        public ConnectionRow GetConnectionRow(object query)
        {
            ConnectionRow result;
            lock(isLock)
            { 
                if (connectionsList == null)
                    connectionsList = new List<ConnectionRow>();
                
                //если нет доступных коннекшинов
                if (connectionsList.Count() == 0 || connectionsList.Where(x => x.queryList.Count() < transactionLimithPerConnection).Count() == 0)
                {
                    result = new ConnectionRow(this, isLock, InitConnection());
                    connectionsList.Add(result);
                }
                else
                {
                    int minTranUse = connectionsList.Where(x => x.queryList.Count() < transactionLimithPerConnection).Select(x => x.queryList.Count()).Min();
                    result = connectionsList.Where(x => x.queryList.Count() == minTranUse).FirstOrDefault();
                }
                if(query!=null)
                    result.queryList.Add(query);
            }
            
            return result;
        }
        /// <summary>
        /// Максимальное количество транзакций, разрешенных для одного соединения
        /// </summary>
        public int transactionLimithPerConnection = 10;


        public DbConnection TryInitConnection(DbConnection connection)
        {
            for (int tryCnt = 0; tryCnt < 10; tryCnt++)
            {
                if (connection.State != ConnectionState.Open)
                {
                    try
                    {
                        while (connection.State == ConnectionState.Connecting)
                        {
                            Thread.Sleep(100);
                        }
                        connection = new SqlConnection(connectionString);
                        connection.Open();
                    }
                    catch (Exception e)
                    {
                        Debug.WriteLine(e.Message);
                        Thread.Sleep(100);
                    }
                }
            }
            return connection;
        }


        public QueryFactoryBase ConfigureConnection(string connectionString, string factoryName)
        {
            this.factoryName = factoryName;
            this.connectionString = connectionString;
            return this;
        }


        public QueryFactoryBase NewFactory(DbConnection connection, string connectionName)
        {
            return this;
        }

        public string logPath
        {
            get
            {
                return AppDomain.CurrentDomain.BaseDirectory + $"/sqlErr{this.GetType().Name}.log";
            }
        }

        public void Dispose()
        {
            foreach (ConnectionRow connectionRow in connectionsList)
            {

                if (connectionRow.GetConnection() != null)
                {
                    connectionRow.GetConnection().Close();
                    connectionRow.GetConnection().Dispose();
                }
            }
            GC.SuppressFinalize(this);
        }

        public abstract DbConnection InitConnection();
    }
}
