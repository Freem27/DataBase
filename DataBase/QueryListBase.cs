using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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
    public abstract class QueryListBase : IDisposable
    {
        public static string NoError = "Ok";
        bool continueIfError;
        public List<string> errList;
        public bool withError;
        public List<QueryBase> queryList;
        public bool writeSqlErrLog = false;

        private DbTransaction _transaction;
        public DbTransaction transaction
        {
            get {
                if (_transaction == null)
                    _transaction = connectionRow.GetConnection().BeginTransaction();
                return _transaction;
            }
        }

        //public DbConnection connection;
        private QueryFactoryBase.ConnectionRow _connectionRow;
        public QueryFactoryBase.ConnectionRow connectionRow {
            get { 
                if (_connectionRow == null)
                    _connectionRow = factory.GetConnectionRow(this);
                return _connectionRow;
            }
        }
        bool manualTransaction;
        public QueryFactoryBase factory;

        public string GetErrorSplitString()
        {
            string result = QueryBase.NoError;
            if (withError)
                result = string.Join(",", errList);
            return result;
        }

        public QueryListBase(QueryFactoryBase factory, bool continueIfError, bool manualTransaction)
        {
            this.factory = factory;
            //transaction = null;
            //this.connection =  factory.GetConnection();
            this.manualTransaction = manualTransaction;
            this.continueIfError = continueIfError;

            errList = new List<string>();
            queryList = new List<QueryBase>();
            withError = false;
        }

        public void AddQuery(QueryBase q)
        {
            if (q != null)
                queryList.Add(q);
        }

        public QueryListBase Execute()
        {
            try
            {
                if (manualTransaction)
                {
                    ExecuteWorker();
                    queryList.Clear();
                }
                else
                {
                    ExecuteWorker();
                    transaction.Commit();
                    connectionRow.ReleaseConnection(this);
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                if(!manualTransaction)
                    connectionRow.ReleaseConnection(this);
            }
            return this;
        }

        public bool ExecuteWorker()
        {
            int rowsAffected = 0;
            int progressShowPeriod = 300;
            int currQueryIndex = 0;
            foreach (QueryBase query in queryList)
            {
                if (string.IsNullOrEmpty(query.sql))
                    continue;
                if (currQueryIndex % progressShowPeriod == 0 && currQueryIndex > 0)
                    Debug.WriteLine($"{currQueryIndex} / {queryList.Count}");
                currQueryIndex++;
                try
                {
                    DbCommand cmd = query.InitDbCommand(transaction.Connection,transaction);
                    rowsAffected += cmd.ExecuteNonQuery();
                    query.error = NoError;

                }
                catch (Exception e)
                {
                    withError = true;
                    query.error = $"{e.Message} {Environment.NewLine} sql: {query.sql} ";
                    if (e.Message.Contains("String or binary data would be truncated"))
                    {
                        foreach (SqlParameter par in query.pars)
                            query.error += $"{par.ParameterName} {par.SqlValue.ToString().Length}\n\r{Environment.NewLine}";
                    }
                    else
                    {
                        foreach (SqlParameter par in query.pars)
                            query.error += Environment.NewLine + par.ParameterName + " = " + par.Value.ToString() + ",";
                    }
                    errList.Add(query.error);
                    if (writeSqlErrLog)
                        Shared.InFileLog(query.error, factory.logPath);
                    if (!continueIfError)
                    {
                        transaction.Rollback();
                        Debug.WriteLine("QueryList.execute() ERROR: " + e.Message);
                        return withError;
                    }
                }
            }
            Debug.WriteLine("QueryList.execute(): " + rowsAffected);
            return withError;
        }

        public void ManualCommit()
        {
            if (transaction != null && transaction.Connection != null)
                transaction.Commit();
            Dispose();
        }

        public void ManualRollback()
        {
            if (transaction != null)
                transaction.Rollback();
            Dispose();
        }

        public void Dispose()
        {
            if (connectionRow != null)
                connectionRow.ReleaseConnection(this);
            this._connectionRow = null;
        }
    }
}
