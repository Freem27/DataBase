using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TDV.DataBase
{

    public abstract class ConnectionBase
    {
        public ConnectionBase(string server, string database, string login, string passw, int timeout = 30, bool useWinAuth = false)
        {
            this.server = server;
            this.database = database;
            this.login = login;
            //this.port = 5432; //дефолтный порт для pgsql
            this.passw = passw;
            this.timeout = timeout;
            this.useWinAuth = useWinAuth;
        }


        //Для Oracle
        //public Connection SetSid(string sid)
        //{
        //    this.sid = sid;
        //    return this;
        //}
        //public Connection SetPort(int port)
        //{
        //    this.port = port;
        //    return this;
        //}

        //public string sid;
        public bool useWinAuth;
        public string server;
        public int port;
        public string database;
        public string login;
        public string passw;
        public int timeout;



        //public NpgsqlConnection GetPgSqlConnection()
        //{
        //    return new NpgsqlConnection(GetPosgtreConnectionString());
        //}

        //public OracleConnection GetOracleConnection()
        //{
        //    if (string.IsNullOrEmpty(sid))
        //        throw new Exception("При объявлении OracleConnection не указан SID");
        //    string connString = $"Data Source = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {server})(PORT = {port}))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = {sid}))){ (string.IsNullOrEmpty(passw) ? "" : ";Password=" + passw)}{(string.IsNullOrEmpty(login) ? "" : "User ID=" + login)}";


        //    return new OracleConnection(connString);
        //}

        //public string GetPosgtreConnectionString()
        //{
        //    return $"Server={server};Port={port};User Id={login};Password={passw};database={database};";
        //}




    }
}
