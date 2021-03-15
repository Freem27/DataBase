using System;
using System.Collections;
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
    public abstract class QueryBase
    {
        public bool manualCommit = false;
        public bool writeSqlErrLog = true;
        //public DbTransaction transaction;
        public bool withError;
        public static string NoError = "Ok";
        public QueryListBase parentList;
        public DbCommand cmd;
        public string sql;
        public int timeout;
        public string error;
        public QueryFactoryBase factory;


        public List<DbParameter> pars;

        public QueryBase(QueryFactoryBase factory)
        {
            //transaction = null;
            this.factory = factory;
            withError = false;
            error = QueryBase.NoError;
            pars = new List<DbParameter>();
            timeout = 180;
        }

        public QueryBase(QueryFactoryBase factory, string sql = "", int timeoutSeconds = 180) : this(factory)
        {
            this.sql = sql;
            timeout = timeoutSeconds;
        }

        public void FillJObjectValue(ref JObject reuslt, DbDataReader reader, string fieldName, int colNum)
        {
            if (reader.IsDBNull(colNum))
                reuslt[fieldName] = null;
            else
            {
                switch (reader.GetFieldType(colNum).Name)
                {
                    case nameof(Decimal):
                        reuslt[fieldName] = reader.GetDecimal(colNum);
                        break;
                    case nameof(Double):
                        reuslt[fieldName] = reader.GetDouble(colNum);
                        break;
                    case nameof(Int16):
                        reuslt[fieldName] = reader.GetInt16(colNum);
                        break;
                    case nameof(Int32):
                        reuslt[fieldName] = reader.GetInt32(colNum);
                        break;
                    case nameof(DateTime):
                        reuslt[fieldName] = reader.GetDateTime(colNum);
                        break;
                    case nameof(Boolean):
                        reuslt[fieldName] = reader.GetBoolean(colNum);
                        break;
                    case "Byte[]":

                        Stream stream = reader.GetStream(colNum);

                        reuslt[fieldName] = System.Convert.ToBase64String(((MemoryStream)stream).ToArray());
                        break;
                    case nameof(String):
                        string val = reader.GetValue(colNum).ToString();
                        if (val == "true" || val == "false")
                            reuslt[fieldName] = (val == "true" ? true : false);
                        else
                            reuslt[fieldName] = val;
                        break;
                    default:
                        reuslt[fieldName] = reader.GetValue(colNum).ToString();
                        break;
                }
            }
        }

        public QueryBase Execute()
        {
            Action<DbConnection> worker = (connection) =>
            {
                try
                {
                    cmd = InitDbCommand(connection,null);
                    int n = cmd.ExecuteNonQuery();
                    Debug.WriteLine("execQuery() rowsAffected: " + n);
                }
                catch (Exception e)
                {
                    WriteError(e.Message);
                }
            };

            
            var conRow = factory.GetConnectionRow(this);
            worker(conRow.GetConnection());
            conRow.ReleaseConnection(this);
            return this;
        }

        public void Abort()
        {
            if (cmd != null)
                cmd.Cancel();
            Debug.WriteLine("task was cancel");
        }

        public T GetFirstCell<T>()
        {
            T value = default;
            Action<DbConnection,DbTransaction> worker = (connection, transaction) => {
                try
                {
                    cmd = InitDbCommand(connection,transaction);
                    DbDataReader reader = cmd.ExecuteReader();
                    if (reader.Read())
                    { 
                        value = (T)reader.GetFieldValue<T>(0);
                    }
                    reader.Close();
                }
                catch (Exception e)
                {
                    WriteError(e.Message);
                }
            };
            if (parentList != null)
                worker(parentList.transaction.Connection, parentList.transaction);
            else
            { 
                var conRow = factory.GetConnectionRow(this);
                worker(conRow.GetConnection(),null);
                conRow.ReleaseConnection(this);
            }
            return value;
        }

        /// <summary>
        /// пример sql:
        /// insert into [TABLE] (name) output inserted.id values ('название') 
        /// </summary>
        public T InsertSqlReturnId<T>(DbTransaction transaction = null) 
        {
            T result = default(T);
            Action<DbConnection> worker = (connection) =>
            {
                try
                {
                    DbCommand cmd = InitDbCommand(connection,transaction);
                    DbDataReader reader = cmd.ExecuteReader();
                    reader.Read();
                    result =reader.GetFieldValue<T>(0); 
                    reader.Close();
                }
                catch (SqlException e)
                {
                    WriteError(e.Message);
                }
            };
            if (transaction != null)
                worker(transaction.Connection);
            else
            {
                var conRow = factory.GetConnectionRow(this);    
                worker(conRow.GetConnection());
                conRow.ReleaseConnection(this);
            }
            return result;
        }

        public override string ToString()
        {
            string parList = string.Join("\n ", pars.Select(x => x.ParameterName + " " + (x.Value == DBNull.Value ? "null" : x.Value.ToString())).ToList());
            return $"sql={sql}"+(string.IsNullOrEmpty(parList)? "":$" params={parList}");
        }

        public abstract DbCommand InitDbCommand(DbConnection connection, DbTransaction transaction);

        public JArray ToJArray(DbTransaction transaction = null)
        {
            JArray result = new JArray();
            Action<DbConnection> worker = (connection) =>
            {
                DbDataReader reader = null;
                try
                {
                    DbCommand cmd = InitDbCommand(connection,transaction);
                    
                    List<string> fields = new List<string>();
                    reader = cmd.ExecuteReader();
                    for (int i = 0; i < reader.FieldCount; i++)
                        fields.Add(reader.GetName(i));
                    while (reader.Read())
                    {
                        JObject row = new JObject();
                        for (int c = 0; c < fields.Count; c++)
                        {
                            FillJObjectValue(ref row, reader, fields[c], c);
                        }
                        result.Add(row);
                    }
                }
                catch (SqlException e)
                {
                    WriteError(e.Message);
                }
                finally
                {
                    if (reader != null)
                        reader.Close();
                }
            };

            if (transaction != null)
                worker(transaction.Connection);
            else
            {
                var conRow = factory.GetConnectionRow(this);
                worker(conRow.GetConnection());
                conRow.ReleaseConnection(this);
            }
            return result;
        }

        public JObject ToJObject(DbTransaction transaction = null)
        {
            JObject result = new JObject();
            Action<DbConnection> worker = (connection) =>
            {
                try
                {
                    DbCommand cmd = InitDbCommand(connection,transaction);
                    List<string> fields = new List<string>();
                    DbDataReader reader = cmd.ExecuteReader();
                    for (int i = 0; i < reader.FieldCount; i++)
                        fields.Add(reader.GetName(i));
                    if (reader.Read())
                        for (int c = 0; c < fields.Count; c++)
                        {
                            FillJObjectValue(ref result, reader, fields[c], c);
                        }
                    reader.Close();
                }
                catch (SqlException e)
                {
                    WriteError(e.Message);
                }
            };
            if (transaction != null)
                worker(transaction.Connection);
            else
            {
                var conRow = factory.GetConnectionRow(this);
                worker(conRow.GetConnection());
                conRow.ReleaseConnection(this);
            }
            return result;
        }

        private void WriteError(string errText)
        {
            withError = true;
            error = errText;
            string errorFullString = $"{errText} {Environment.NewLine} sql: {ToString()} ";
            Debug.WriteLine(errorFullString);
            if (writeSqlErrLog)
                Shared.InFileLog(errorFullString, factory.logPath);
        }

        public DataTable ToDataTable(DbTransaction transaction = null)
        {
            DataTable result = new DataTable();
            Action<DbConnection> worker = (connection) =>
            {
                DbDataReader reader = null;
                try
                {
                    DbCommand cmd = InitDbCommand(connection,transaction);
                    reader = cmd.ExecuteReader();
                    result.Load(reader);
                    foreach (DataColumn col in result.Columns)
                        col.ReadOnly = false;
                }
                catch (SqlException e)
                {
                    WriteError(e.Message);
                }
                finally
                {
                    if (reader != null)
                        reader.Close();
                }
            };
            if (transaction != null)
                worker(transaction.Connection);
            else
            {
                var conRow = factory.GetConnectionRow(this);
                worker(conRow.GetConnection());
                conRow.ReleaseConnection(this);
            }
            return result;
        }


        /// <summary>
        /// Заменяет все пустые ячейки в DataTable на Null
        /// </summary>
        public void ReplaceDataTableSpaces(DataTable data)
        {
            List<int> stringColums = new List<int>();
            foreach (DataColumn col in data.Columns)
                if (col.DataType == typeof(string))
                    stringColums.Add(data.Columns.IndexOf(col));

            foreach (DataRow row in data.Rows)
            {
                foreach (int col in stringColums)
                    if (string.IsNullOrEmpty(row[col].ToString()))
                        row[col] = DBNull.Value;
            }
        }

        public virtual QueryBase AddParameter(string name, object value, bool replaceSpaceWithNull = true, bool parseDate = false)
        {
            bool setNull = false;
            try
            {
                if (parseDate && value != null)
                {
                    DateTime parsed;
                    if (DateTime.TryParse(value.ToString(), out parsed))
                        value = parsed;
                    else value = "";
                }
                if (value != null && value is JValue)
                    value = value.ToString();
                
                if (replaceSpaceWithNull && (
                    value == null
                    || string.IsNullOrEmpty(value.ToString())
                    || (value.GetType().Equals(typeof(DateTime)) && ((DateTime)value).Year == 1))
                    )
                    setNull = true;

                foreach (SqlParameter p in pars)
                    if (p.ParameterName == name)
                    {
                        if (setNull)
                            p.Value = DBNull.Value;
                        else
                            p.Value = value;
                        return this;
                    }

                if (setNull)
                    pars.Add(new SqlParameter(name, DBNull.Value));
                else
                {
                    SqlParameter newPar = new SqlParameter(name, value);
                    if (value.GetType().Equals(typeof(DateTime)) && ((DateTime)value).Year <= 1753)  //если год меньше 1753, то это тип данных Date
                        newPar.SqlDbType = SqlDbType.Date;
                    pars.Add(newPar);
                }
            }
            catch (Exception e)
            {
                if (writeSqlErrLog)
                    Shared.InFileLog(e.Message, factory.logPath);
            }
            return this;
        }


        public virtual QueryBase AddParametersList<T>(string name, IEnumerable<T> values)
        {
            bool setNull = false;
            //throw new NotImplementedException("Необходимо релизовать метод AddParametersList<T> в классе наследнике");
            try
            {
                if (values.Count() == 0)
                    setNull = true;

                if (setNull)
                    pars.Add(new SqlParameter(name, DBNull.Value));
                else
                {
                    List<string> parametersNames = new List<string>();
                    int parIndex = 0;
                    foreach(var value in values)
                    {
                        parametersNames.Add(name + parIndex.ToString());
                        SqlParameter newPar = new SqlParameter(name + parIndex.ToString(), value) ;
                        parIndex++;
                        pars.Add(newPar);
                    }
                    sql=sql.Replace(name, string.Join(",", parametersNames));
                }
            }
            catch (Exception e)
            {
                if (writeSqlErrLog)
                    Shared.InFileLog(e.Message, factory.logPath);
            }
            return this;
        }
    }
}
