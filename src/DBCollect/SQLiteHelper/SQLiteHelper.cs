using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace SQLiteHelper
{
    public static class SQLiteHelper
    {
        readonly static string ParamsPattern = "@[a-zA-Z-_]+\\b";
        public static string connectionString = "Data Source=" + AppDomain.CurrentDomain.BaseDirectory + "\\" + ConfigurationSettings.AppSettings["ConnectString"];

        private static void PrepareCommand(SQLiteCommand command, SQLiteConnection connection, SQLiteTransaction transaction, string commandText, params SQLiteParameter[] commandParameters)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (commandText == null || commandText.Length == 0) throw new ArgumentNullException("commandText");

            // 给命令分配一个数据库连接. 
            command.Connection = connection;

            // 设置命令文本(存储过程名或SQL语句) 
            command.CommandText = commandText;

            // 分配事务 
            if (transaction != null)
            {
                if (transaction.Connection == null) throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
                command.Transaction = transaction;
            }

            // 设置命令类型. 
            command.CommandType = CommandType.Text;

            // 分配命令参数 
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }
        }
        private static void AttachParameters(SQLiteCommand command, SQLiteParameter[] commandParameters)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (commandParameters != null)
            {
                foreach (SQLiteParameter p in commandParameters)
                {
                    if (p != null)
                    {
                        // 检查未分配值的输出参数,将其分配以DBNull.Value. 
                        if ((p.Direction == ParameterDirection.InputOutput || p.Direction == ParameterDirection.Input) &&
                            (p.Value == null))
                        {
                            p.Value = DBNull.Value;
                        }
                        command.Parameters.Add(p);
                    }
                }
            }
        }

        private static SQLiteParameter[] CreateParams(string commandText, params object[] datas)
        {
            if (datas is SQLiteParameter[])
                return (SQLiteParameter[])datas;

            var matches = Regex.Matches(commandText, "@[a-zA-Z-_]+\\b");
            List<SQLiteParameter> paramList = new List<SQLiteParameter>();
            if (matches.Count == datas.Length)
            {
                int i = 0;
                foreach (Match match in matches)
                {
                    paramList.Add(new SQLiteParameter(match.Value, datas[i]));
                    i++;
                }
            }
            return paramList.ToArray();
        }

        #region 执行数据库操作(新增、更新或删除)，返回影响行数
        /// <summary>
        /// 执行数据库操作(新增、更新或删除)
        /// </summary>
        /// <param name="cmd">SqlCommand对象</param>
        /// <returns>所受影响的行数</returns>
        public static int ExecuteNonQueryTrans(string commandText, params object[] commandParamters)
        {
            int result = 0;
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");

            SQLiteCommand cmd = new SQLiteCommand();
            using (SQLiteConnection con = new SQLiteConnection(connectionString))
            {
                con.Open();
                SQLiteTransaction trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                PrepareCommand(cmd, con, trans, commandText, CreateParams(commandText, commandParamters));
                try
                {
                    result = cmd.ExecuteNonQuery();
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    throw ex;
                }
            }
            return result;
        }


        /// <summary>
        /// 执行数据库操作(新增、更新或删除)
        /// </summary>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="cmdParms"></param>
        /// <returns></returns>
        public static int ExecuteNonQuery(string commandText, params object[] commandParamters)
        {
            int result = 0;
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");

            SQLiteCommand cmd = new SQLiteCommand();
            using (SQLiteConnection con = new SQLiteConnection(connectionString))
            {
                PrepareCommand(cmd, con, null, commandText, CreateParams(commandText, commandParamters));
                result = cmd.ExecuteNonQuery();
            }
            return result;
        }
        #endregion

        #region 执行数据库操作(新增、更新或删除)同时返回执行后查询所得的第1行第1列数据
        /// <summary>
        /// 执行数据库操作(新增、更新或删除)同时返回执行后查询所得的第1行第1列数据
        /// </summary>
        /// <param name="cmd">SqlCommand对象</param>
        /// <returns>查询所得的第1行第1列数据</returns>
        public static object ExecuteScalarTrans(string commandText, params object[] commandParamters)
        {
            object result = null;
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");

            SQLiteCommand cmd = new SQLiteCommand();
            using (SQLiteConnection con = new SQLiteConnection(connectionString))
            {
                con.Open();
                SQLiteTransaction trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                PrepareCommand(cmd, con, trans, commandText, CreateParams(commandText, commandParamters));
                try
                {
                    result = cmd.ExecuteScalar();
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    throw ex;
                }
            }
            return result;
        }

        /// <summary>
        /// 执行数据库操作(新增、更新或删除)同时返回执行后查询所得的第1行第1列数据
        /// </summary>
        /// <param name="commandText">执行语句或存储过程名</param>
        /// <param name="commandType">执行类型（默认语句）</param>
        /// <returns>查询所得的第1行第1列数据</returns>
        public static object ExecuteScalar(string commandText, params object[] commandParamters)
        {
            object result = null;
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");

            SQLiteCommand cmd = new SQLiteCommand();
            using (SQLiteConnection con = new SQLiteConnection(connectionString))
            {
                con.Open();
                PrepareCommand(cmd, con, null, commandText, CreateParams(commandText, commandParamters));
                result = cmd.ExecuteScalar();
            }
            return result;
        }
        #endregion

        #region 执行数据库查询，返回SqlDataReader对象

        /// <summary>
        /// 执行数据库查询，返回SqlDataReader对象
        /// </summary>
        /// <param name="commandText">执行语句或存储过程名</param>
        /// <param name="commandType">执行类型（默认语句）</param>
        /// <param name="cmdParms">SQL参数对象</param>
        /// <returns>SqlDataReader对象</returns>
        public static DbDataReader ExecuteReader(string commandText, params SQLiteParameter[] cmdParms)
        {
            DbDataReader reader = null;
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");

            SQLiteConnection con = new SQLiteConnection(connectionString);
            SQLiteCommand cmd = new SQLiteCommand();
            PrepareCommand(cmd, con, null, commandText, CreateParams(commandText, cmdParms));
            try
            {
                reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return reader;
        }
        #endregion

        #region 执行数据库查询，返回DataSet对象
     
        /// <summary>
        /// 执行数据库查询，返回DataSet对象
        /// </summary>
        /// <param name="commandText">执行语句或存储过程名</param>
        /// <param name="commandType">执行类型(默认语句)</param>
        /// <param name="cmdParms">SQL参数对象</param>
        /// <returns>DataSet对象</returns>
        public static DataSet ExecuteDataSet(string commandText, params SQLiteParameter[] cmdParms)
        {
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");
            DataSet ds = new DataSet();
            using (SQLiteConnection con = new SQLiteConnection(connectionString))
            {
                con.Open();

                SQLiteCommand cmd = new SQLiteCommand();
                PrepareCommand(cmd, con, null, commandText, CreateParams(commandText, cmdParms));
                SQLiteDataAdapter sda = new SQLiteDataAdapter(cmd);
                
                sda.Fill(ds);
            }
            return ds;
        }
        #endregion

        #region 执行数据库查询，返回DataTable对象
        /// <summary>
        /// 执行数据库查询，返回DataTable对象
        /// </summary>
        /// <param name="commandText">执行语句或存储过程名</param>
        /// <param name="commandType">执行类型(默认语句)</param>
        /// <returns>DataTable对象</returns>
        public static DataTable ExecuteDataTable(string commandText, params SQLiteParameter[] cmdParms)
        {
            if (connectionString == null || connectionString.Length == 0)
                throw new ArgumentNullException("connectionString");
            if (commandText == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");
            DataTable dt = new DataTable();
            using (SQLiteConnection con = new SQLiteConnection(connectionString))
            {
                con.Open();

                SQLiteCommand cmd = new SQLiteCommand();
                PrepareCommand(cmd, con, null, commandText, CreateParams(commandText, cmdParms));
                SQLiteDataAdapter sda = new SQLiteDataAdapter(cmd);

                sda.Fill(dt);
            }
            return dt;
        }

        #endregion

        #region 通用分页查询方法
        /// <summary>
        /// 通用分页查询方法
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strColumns">查询字段名</param>
        /// <param name="strWhere">where条件</param>
        /// <param name="strOrder">排序条件</param>
        /// <param name="pageSize">每页数据数量</param>
        /// <param name="currentIndex">当前页数</param>
        /// <param name="recordOut">数据总量</param>
        /// <returns>DataTable数据表</returns>
        public static DataTable SelectPaging(string tableName, string strColumns, string strWhere, string strOrder, int pageSize, int currentIndex, out int recordOut)
        {
            DataTable dt = new DataTable();
            recordOut = Convert.ToInt32(ExecuteScalar("select count(*) from " + tableName, CommandType.Text));
            string pagingTemplate = "select {0} from {1} where {2} order by {3} limit {4} offset {5} ";
            int offsetCount = (currentIndex - 1) * pageSize;
            string commandText = String.Format(pagingTemplate, strColumns, tableName, strWhere, strOrder, pageSize.ToString(), offsetCount.ToString());
            using (DbDataReader reader = ExecuteReader(commandText))
            {
                if (reader != null)
                {
                    dt.Load(reader);
                }
            }
            return dt;
        }

        #endregion

    }
}