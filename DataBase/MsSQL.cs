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
    public class MsQuery : QueryBase
    {
        //public MsQueryList parentList = null;
        public MsQuery(QueryFactoryBase factory, string sql, int timeoutSeconds) : base(factory, sql, timeoutSeconds) { }

        public class _WithForJson
        {
            private MsQuery parentQuery;
            public string jsonForParse; //данные из запроса для объединения
            public string error { get { return parentQuery.error; } }
            public bool withError { get { return parentQuery.withError; } }

            public _WithForJson(MsQuery parent)
            {
                jsonForParse = "";
                parentQuery = parent;
                DataTable data = null;
                data = parent.ToDataTable();
                if (data != null && data.Rows.Count > 0)
                {
                    jsonForParse = string.Join("", data.Rows.Cast<DataRow>().Select(x => x[0].ToString()).ToList());
                }
            }

            /// <summary>
            /// sql синтаксис: обязательно without_array_wrapper для root объекта запроса
            /// </summary>
            /// <returns></returns>
            public JObject ToJObject()
            {
                JObject result = new JObject();
                try
                {
                    if (!string.IsNullOrEmpty(jsonForParse))
                        result = JObject.Parse(jsonForParse);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    parentQuery.error = e.Message;
                    parentQuery.withError = true;
                }
                return result;
            }


            /// <summary>
            /// sql синтаксис: обязательно без without_array_wrapper для root объекта запроса
            /// </summary>
            /// <returns></returns>
            public JArray ToJArray()
            {
                JArray result = new JArray();
                try
                {
                    if (!string.IsNullOrEmpty(jsonForParse))
                        result = JArray.Parse(jsonForParse);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    parentQuery.error = e.Message;
                    parentQuery.withError = true;
                }
                return result;
            }
        }
        /// <summary>
        /// разбирает ответ MS SQL сервер в виде JSON
        /// sql синтаксис: FOR JSON PATH,INCLUDE_NULL_VALUES
        /// </summary>
        public _WithForJson WithForJson()
        {
            _WithForJson result = new _WithForJson(this);
            
            return result;
        }

        /// <summary>
        /// выполнение insert\update запроса
        /// </summary>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public MsQuery Execute()
        {
            return (MsQuery)base.Execute();
        }

        public override DbCommand InitDbCommand(DbConnection connection, DbTransaction transaction)
        {
            SqlCommand cmd = null;
            if (transaction != null)
            {
                cmd = new SqlCommand(sql, (SqlConnection)transaction.Connection, (SqlTransaction)transaction);
            }
            else
            {
                cmd = new SqlCommand(sql, (SqlConnection)connection);
            }
            cmd.CommandTimeout = timeout;
            foreach (SqlParameter par in pars)
                cmd.Parameters.Add(par);
            return cmd;
        }

        public MsQuery AddParameter(string name, object value, bool replaceSpaceWithNull = true, bool parseDate = false)
        {
            return (MsQuery)base.AddParameter(name, value, replaceSpaceWithNull, parseDate);
        }

        public MsQuery AddParametersList<T>(string name, IEnumerable<T> values)
        {
            return (MsQuery)base.AddParametersList<T>(name, values);
        }


        public MsQuery BulkInsert(DataTable data, bool replaceSpaceWithNull = true)
        {
            if (string.IsNullOrEmpty(data.TableName))
            {
                error = "TableName в DataTable data должно быть заполнено";
                withError = true;
            }
            else
                BulkInsert(data.TableName, data, replaceSpaceWithNull);
            return this;
        }

        public MsQuery BulkInsert(string tableName, DataTable data, bool replaceSpaceWithNull = true)
        {
            int bulkRowsTotal = data.Rows.Count;
            if (replaceSpaceWithNull)
                ReplaceDataTableSpaces(data);

            Action<SqlConnection,SqlTransaction> worker = (connection,transaction) =>
            {
                //if (connection.State != ConnectionState.Open)
                //    connection.Open();

                SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, transaction);
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.NotifyAfter = 10000;
                long rowCopiedNotified = -1;
                bulkCopy.SqlRowsCopied += (sender, e) =>
                {
                    rowCopiedNotified = e.RowsCopied;
                    Debug.WriteLine($"bulkInsert({tableName}) {e.RowsCopied}/{bulkRowsTotal}");
                };
                bulkCopy.BulkCopyTimeout = 0;
                try
                {
                    bulkCopy.WriteToServer(data);
                    Debug.WriteLine($"bulkInsert({tableName}) " + (rowCopiedNotified != bulkRowsTotal ? $"{bulkRowsTotal}/{bulkRowsTotal}" : "") + " finished");
                }

                catch (Exception ex)
                {
                    withError = true;
                    error = ex.Message;
                    Debug.WriteLine($"bulkInsert({tableName}) ERROR: " + ex.Message);
                }
            };

            //этот код нужен для выполнения в рамках QueryList

            if (parentList != null ) 
            {                
                worker((SqlConnection)parentList.transaction.Connection, (SqlTransaction)parentList.transaction);
            }
            else
            {
                var conRow = factory.GetConnectionRow(this);
                worker((SqlConnection)conRow.GetConnection(),null);
                conRow.ReleaseConnection(this);
            }
            return this;
        }

        public MsQuery SetTimeout(int timeoutSeconds)
        {
            this.timeout = timeoutSeconds;
            return this;
        }
    }

    public class MsQueryFactory : QueryFactoryBase
    {
        private static MsQueryFactory _instance;

        public static MsQueryFactory Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new MsQueryFactory();
                return _instance;
            }
        }

        private MsQueryFactory()
        {
        }

        public static MsQuery NewQuery(string sql, int timeoutSeconds = 180)
        {
            MsQuery result = new MsQuery(Instance, sql, timeoutSeconds);
            return result;
        }

        public static MsQueryList NewQueryList(bool continueIfError = false, bool manualTransaction = false)
        {
            MsQueryList result = new MsQueryList(Instance, continueIfError, manualTransaction);
            return result;
        }

        public MsQuery NewQuery(string sql)
        {
            MsQuery result = new MsQuery(this, sql, 180);
            return result;
        }



        public static MsQueryFactory NewFactory(string connectionString, string factoryName)
        {
            MsQueryFactory newFactory = new MsQueryFactory();
            newFactory.connectionString = connectionString;
            newFactory.factoryName = factoryName;
            return newFactory;
        }

        public override DbConnection InitConnection()
        {
            return new SqlConnection(connectionString);
        }

        //public MsQueryFactory ToMsQueryFactory(this QueryFactoryBase factory)
        //{
        //    return (MsQueryFactory)factory;
        //}


    }


    public class MsQueryList : QueryListBase
    {
        /// <summary>
        /// 
        /// Соединение с БД и Транзакция отрываются при первом вызове execute() или bulkInsert()
        /// </summary>
        /// <param name="continueIfError">true: Если один из запросов выполнится с ошибкой не прерывать выполнение. false: после первой ошибки выполнить rollback</param>
        /// <param name="con"></param>
        /// <param name="manualTransaction">Ручной commit и rollback. после каждого Execute() очищается список queryList</param>
        public MsQueryList(MsQueryFactory factory, bool continueIfError = false, bool manualTransaction = false) : base(factory, continueIfError, manualTransaction)
        {
            this.factory = factory;
        }

        public MsQuery AddNewQuery(string sql = "", int timeout = 180, bool addToQueryList = true)
        {
            MsQuery q = ((MsQueryFactory)factory).NewQuery(sql);
            q.timeout = timeout;
            q.manualCommit = true;
            q.parentList = this;
            //if (transaction != null)
            //    q.transaction = transaction;
            if (addToQueryList)
                queryList.Add(q);
            return q;
        }
    }


    public static class MsQueryExtentions
    {
        public static MsQuery ToMsQuery(this QueryBuilder builder, MsQueryFactory factory = null)
        {
            if (factory == null)
                factory = MsQueryFactory.Instance;

            MsQuery result = factory.NewQuery("");
            result.timeout = builder.timeoutSeconds;
            string setPart = ""; //Для update
            string varList = ""; //для insert 
            string parList = ""; //для insert
            int parIndex = 0;
            switch (builder.mode)
            {
                case QueryBuilder.BuilderType.update:
                    if (builder.upsertParametrs.Count == 0 && builder.specialParameters.Count==0)
                    {
                        builder.error = "Не задан ни один параметр для обновления";
                        return null;
                    }
                    foreach (QueryBuilder.SQLparametr par in builder.upsertParametrs)
                    {
                        string varName = $"@parametr{parIndex}";
                        parIndex++;
                        setPart += $",{par.name}={varName}";
                        result.AddParameter(varName, par.value, par.replaceSpaceWithNull, par.parseDate);
                    }

                    foreach (KeyValuePair<string, QueryBuilder.SPECIAL_PARAMETER> par in builder.specialParameters)
                    {
                        switch(par.Value)
                        {
                            case QueryBuilder.SPECIAL_PARAMETER.CURRENT_TIMESTAMP:
                                setPart += $",{par.Key}=current_timestamp";
                                break;
                            default:
                                throw new Exception("Не задана логика для SPECIAL_PARAMETER="+par.Value.ToString());
                        }
                    }

                    if (setPart.Length > 0 && setPart[0] == ',')
                        setPart = setPart.Remove(0, 1);

                    result.sql = $"update {builder.tableName} set {setPart} {builder.whereSqlPart}";
                    break;
                case QueryBuilder.BuilderType.insert:
                    if (builder.upsertParametrs.Count == 0 && builder.specialParameters.Count == 0)
                    {
                        builder.error = "Не задан ни один параметр для обновления";
                        return null;
                    }
                    foreach (QueryBuilder.SQLparametr par in builder.upsertParametrs)
                    {
                        string varName = $"@parametr{parIndex}";
                        parIndex++;
                        parList += $",{par.name}";
                        varList += $",{varName}";
                        result.AddParameter(varName, par.value, par.replaceSpaceWithNull, par.parseDate);
                    }

                    foreach (KeyValuePair<string, QueryBuilder.SPECIAL_PARAMETER> par in builder.specialParameters)
                    {
                        parList += $",{par.Key}";
                        switch (par.Value)
                        {
                            case QueryBuilder.SPECIAL_PARAMETER.CURRENT_TIMESTAMP:

                                varList += $",current_timestamp";
                                break;
                            default:
                                throw new Exception("Не задана логика для SPECIAL_PARAMETER=" + par.Value.ToString());
                        }
                    }

                    if (parList.Length > 0 && parList[0] == ',')
                        parList = parList.Remove(0, 1);
                    if (varList.Length > 0 && varList[0] == ',')
                        varList = varList.Remove(0, 1);

                    string outputString = "";
                    if (!string.IsNullOrEmpty(builder.returningValue))
                    {
                        outputString = $" output inserted.{builder.returningValue}";
                    }

                    result.sql = $"insert into {builder.tableName}  ({parList}){outputString} values ({varList})";

                    break;
                default:
                    break;
            }
            foreach (QueryBuilder.SQLparametr par in builder.pars)
                result.AddParameter(par.name, par.value, par.replaceSpaceWithNull, par.parseDate);
            return result;
        }
    }


    public class MsConnection : ConnectionBase
    {
        public MsConnection(string server, string database, string login, string passw, int timeout = 30, bool useWinAuth = false)
            : base(server, database, login, passw, timeout, useWinAuth)
        { }
        
        public string ToConnectionString()
        {
            if (useWinAuth)
                return $"Server={server};Database={database};Integrated Security=SSPI;persist security info=True; Connection Timeout={timeout}; MultipleActiveResultSets=True";
            else
                return $"Server={server};Database={database};User ID={login};Password={passw}; Connection Timeout={timeout}; MultipleActiveResultSets=True";
        }
        public SqlConnection ToConnection()
        {
            return new SqlConnection(ToConnectionString());
        }
    }
}
