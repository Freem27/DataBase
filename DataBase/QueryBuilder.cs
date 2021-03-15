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

namespace TDV.DataBase
{
    public class QueryBuilder
    {
        public string error;
        public class SQLparametr
        {
            public SQLparametr(string name, object value, bool replaceSpaceWithNull = true, bool parseDate = false)
            {
                this.name = name;
                this.value = value;
                this.replaceSpaceWithNull = replaceSpaceWithNull;
                this.parseDate = parseDate;
            }
            public string name;
            public object value;
            public bool replaceSpaceWithNull;
            public bool parseDate;
        }
        /// <summary>
        /// insertReturnInserted - Обязательно использование метода AddReturningValue
        /// </summary>
        public enum BuilderType { update, insert };
        public BuilderType mode;
        public string tableName;
        public string whereSqlPart;
        /// <summary>
        /// Возвращаемое при Insert значение. допукается возвращать не более одного значения
        /// </summary>
        public string returningValue;
        public enum SPECIAL_PARAMETER { CURRENT_TIMESTAMP };
        public List<SQLparametr> upsertParametrs;
        public List<KeyValuePair<string, SPECIAL_PARAMETER>> specialParameters;
        public List<SQLparametr> pars;
        public int timeoutSeconds;
        public bool HasUpsertParametrs()
        {
            return upsertParametrs.Count > 0 || specialParameters.Count>0;
        }
        SqlConnection connection;
        public QueryBuilder(BuilderType mode = BuilderType.update, int timeoutSeconds = 180, SqlConnection connection = null)
        {
            error = QueryBase.NoError;
            this.mode = mode;
            this.timeoutSeconds = timeoutSeconds;
            this.connection = connection;
            returningValue = null;
            upsertParametrs = new List<SQLparametr>();
            specialParameters = new List<KeyValuePair<string, SPECIAL_PARAMETER>>();
            pars = new List<SQLparametr>();
        }

        public QueryBuilder SetMode(BuilderType mode)
        {
            this.mode = mode;
            return this;
        }

        /// <summary>
        /// Используется для формирования insert зароса. При указании добавляет конструкцию Output inserted.value, где value название возвращаемого столбца
        /// </summary>
        /// <param name="value">Название возвращаемого столбца</param>
        /// <returns></returns>
        public QueryBuilder AddReturningValue(string value)
        {
            returningValue = value;
            return this;
        }

        /// <summary>
        /// используется для формирования sql Update запроса
        /// </summary>
        /// <param name="name">название колонки SQL таблицы</param>
        /// <param name="value">значение</param>
        /// <param name="replaceSpaceWithNull">заменять пустые строки на NULL</param>
        /// <param name="parseDate">попытаться распознать дату</param>
        /// <returns></returns>
        public QueryBuilder AddUpsertParametr(string name, object value, bool replaceSpaceWithNull = true, bool parseDate = false)
        {
            //если параметр name существует - удалить его
            var t= upsertParametrs.Where(p => p.name == name);
            if (t.Count() > 0)
                upsertParametrs.Remove(t.First());
            upsertParametrs.Add(new SQLparametr(name, value, replaceSpaceWithNull, parseDate));
            return this;
        }

        public QueryBuilder AddUpsertParametr(string name, object value, object oldValue, bool replaceSpaceWithNull = true, bool parseDate = false)
        {
            if (value == null && oldValue == null)
                return this;
            if ((value==null && oldValue!=null)|| (value!=null && oldValue==null) || (  value.ToString() != oldValue.ToString()))
                upsertParametrs.Add(new SQLparametr(name, value, replaceSpaceWithNull, parseDate));
            return this;
        }

        public QueryBuilder AddUpsertParametr(string name, SPECIAL_PARAMETER parametr)
        {
            specialParameters.Add(new KeyValuePair<string, SPECIAL_PARAMETER>(name, parametr));
            return this;
        }

        /// <summary>
        /// указание ключевого слова WHERE обязательно
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public QueryBuilder SetWhereSqlPart(string sql)
        {
            whereSqlPart = sql;
            return this;
        }

        /// <summary>
        /// использовать для добавления параметров из части WHERE
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <param name="replaceSpaceWithNull"></param>
        /// <param name="parseDate"></param>
        /// <returns></returns>
        public QueryBuilder AddParametr(string name, object value, bool replaceSpaceWithNull = true, bool parseDate = false)
        {
            pars.Add(new SQLparametr(name, value, replaceSpaceWithNull, parseDate));
            return this;
        }
        /// <summary>
        /// название таблицы (с названием бд и схемы)
        /// </summary>
        /// <returns></returns>
        public QueryBuilder SetTableName(string tableName)
        {
            this.tableName = tableName;
            return this;
        }


    }
}
