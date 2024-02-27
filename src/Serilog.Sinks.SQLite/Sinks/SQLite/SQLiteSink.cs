// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.SQLite
{
    internal class SQLiteSink : BatchProvider, ILogEventSink
    {
        private readonly string _databasePath;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly bool _rollOver;
        private readonly string _tableName;
        private readonly string _password;
        private const string _timestampFormat = "yyyy-MM-ddTHH:mm:ss.fff";
        private static SemaphoreSlim _semaphoreSlim = new(1, 1);
        
        public SQLiteSink(
            string sqlLiteDbPath,
            string tableName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            string password,
            uint batchSize = 100,
            bool rollOver = true) : base(batchSize: (int)batchSize, maxBufferSize: 100_000)
        {
            _databasePath = sqlLiteDbPath;
            _tableName = tableName;
            _formatProvider = formatProvider;
            _storeTimestampInUtc = storeTimestampInUtc;
            _rollOver = rollOver;
            _password = password;

            InitializeDatabase();

        }

        #region ILogEvent implementation

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private void InitializeDatabase()
        {
            using var conn = GetSqliteConnection();
            CreateSqlTable(conn);
        }

        private SqliteConnection GetSqliteConnection()
        {
            var sqlConString = new SqliteConnectionStringBuilder
            {
                DataSource = _databasePath,
                Password = _password,
                Mode = SqliteOpenMode.ReadWriteCreate
            }
            .ConnectionString;

            var sqliteConnection = new SqliteConnection(sqlConString);
            sqliteConnection.Open();

            return sqliteConnection;
        }

        private void CreateSqlTable(SqliteConnection sqlConnection)
        {
            var colDefs = "id INTEGER PRIMARY KEY AUTOINCREMENT,";
            colDefs += "Timestamp TEXT,";
            colDefs += "Level VARCHAR(10),";
            colDefs += "Exception TEXT,";
            colDefs += "MessageTemplate TEXT,";
            colDefs += "RenderedMessage TEXT,";
            colDefs += "Properties TEXT";
            colDefs += "ClassName TEXT";
            colDefs += "MethodName TEXT";
            colDefs += "LineNumber TEXT";

            var sqlCreateText = $"CREATE TABLE IF NOT EXISTS {_tableName} ({colDefs})";

            var sqlCommand = new SqliteCommand(sqlCreateText, sqlConnection);
            sqlCommand.ExecuteNonQuery();
        }

        private SqliteCommand CreateSqlInsertCommand(SqliteConnection connection)
        {
            var sqlInsertText = "INSERT INTO {0} (Timestamp, Level, Exception, MessageTemplate, RenderedMessage, Properties, ClassName, MethodName, LineNumber)";
            sqlInsertText += " VALUES (@timeStamp, @level, @exception, @messageTemplate, @renderedMessage, @properties, @className, @methodName, @lineNumber)";
            sqlInsertText = string.Format(sqlInsertText, _tableName);

            var sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInsertText;
            sqlCommand.CommandType = CommandType.Text;

            sqlCommand.Parameters.Add(new SqliteParameter("@timeStamp", DbType.DateTime2));
            sqlCommand.Parameters.Add(new SqliteParameter("@level", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@exception", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@messageTemplate", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@renderedMessage", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@properties", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@className", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@methodName", DbType.String));
            sqlCommand.Parameters.Add(new SqliteParameter("@lineNumber", DbType.String));

            return sqlCommand;
        }

        private void TruncateLog(SqliteConnection sqlConnection)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName}";
            cmd.ExecuteNonQuery();

            VacuumDatabase(sqlConnection);
        }

        private static void VacuumDatabase(SqliteConnection sqlConnection)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"vacuum";
            cmd.ExecuteNonQuery();
        }

        private SqliteCommand CreateSqlDeleteCommand(SqliteConnection sqlConnection, DateTimeOffset epoch)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName} WHERE Timestamp < @epoch";
            cmd.Parameters.Add(
                new SqliteParameter("@epoch", DbType.DateTime2)
                {
                    Value = (_storeTimestampInUtc ? epoch.ToUniversalTime() : epoch).ToString(
                        _timestampFormat)
                });

            return cmd;
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
            {
                return true;
            }

            await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                using var sqlConnection = GetSqliteConnection();
                try
                {
                    WriteToDatabase(logEventsBatch, sqlConnection);
                    return true;
                }
                catch (SqliteException e)
                {
                    SelfLog.WriteLine(e.Message);

                    if (e.SqliteErrorCode != 3) // https://www.sqlite.org/rescode.html#full
                    {
                        return false;
                    }

                    if (_rollOver == false)
                    {
                        SelfLog.WriteLine("Discarding log excessive of max database");

                        return true;
                    }

                    var dbExtension = Path.GetExtension(_databasePath);

                    var newFilePath = Path.Combine(Path.GetDirectoryName(_databasePath) ?? "Logs",
                        $"{Path.GetFileNameWithoutExtension(_databasePath)}-{DateTime.Now:yyyyMMdd_HHmmss.ff}{dbExtension}");

                    File.Copy(_databasePath, newFilePath, true);

                    TruncateLog(sqlConnection);
                    WriteToDatabase(logEventsBatch, sqlConnection);

                    SelfLog.WriteLine($"Rolling database to {newFilePath}");
                    return true;
                }
                catch (Exception e)
                {
                    SelfLog.WriteLine(e.Message);
                    return false;
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        private void WriteToDatabase(ICollection<LogEvent> logEventsBatch, SqliteConnection sqlConnection)
        {
            using var tr = sqlConnection.BeginTransaction();
            using var sqlCommand = CreateSqlInsertCommand(sqlConnection);
            sqlCommand.Transaction = tr;

            foreach (var logEvent in logEventsBatch)
            {
                sqlCommand.Parameters["@timeStamp"].Value = _storeTimestampInUtc
                    ? logEvent.Timestamp.ToUniversalTime().ToString(_timestampFormat)
                    : logEvent.Timestamp.ToString(_timestampFormat);
                sqlCommand.Parameters["@level"].Value = logEvent.Level.ToString();
                sqlCommand.Parameters["@exception"].Value =
                    logEvent.Exception?.ToString() ?? string.Empty;
                sqlCommand.Parameters["@messageTemplate"].Value = logEvent.MessageTemplate.Text;
                sqlCommand.Parameters["@renderedMessage"].Value = logEvent.RenderMessage(_formatProvider);
                sqlCommand.Parameters["@properties"].Value = logEvent.Properties.Count > 0
                    ? logEvent.Properties.Json()
                    : string.Empty;
                sqlCommand.Parameters["@className"].Value = logEvent.Properties.ContainsKey("ClassName")
                    ? logEvent.Properties["ClassName"]
                    : string.Empty;
                sqlCommand.Parameters["@methodName"].Value = logEvent.Properties.ContainsKey("MethodName")
                    ? logEvent.Properties["MethodName"]
                    : string.Empty;
                sqlCommand.Parameters["@lineNumber"].Value = logEvent.Properties.ContainsKey("LineNumber")
                    ? logEvent.Properties["LineNumber"]
                    : string.Empty;

                sqlCommand.ExecuteNonQuery();
            }
            tr.Commit();
        }
    }
}
