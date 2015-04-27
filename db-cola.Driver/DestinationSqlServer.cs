using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace db_cola.Driver
{
	public class DestinationSqlServer
	{
		public string Alias { get; set; }
		private readonly ConnectionString _connectionString;
		private readonly ILogger _logger;
		private SqlConnection _connection;

		public DestinationSqlServer(string a_ConnString, ILogger a_Logger)
		{
			_connectionString = new ConnectionString(a_ConnString);
			Alias = _connectionString.serverName;
			_logger = a_Logger;
		}

		public void UpdateDatabase(List<Script> a_Scripts)
		{
			try
			{
				_logger.WriteEntry(String.Format("\nPreparing SQL scripts for execution on server {0} ...", Alias));
				_logger.WriteEntry(String.Format("on database {0} ...", _connectionString.databaseName));

				Connect();

				_logger.WriteEntry("\nExecuting scripts ...");


				var database = new SqlDatabase(_connectionString, _connection);
				foreach (var script in a_Scripts)
				{
					_logger.WriteEntry(script.GetFileName());
					database.Update(script);
				}
			}
			finally
			{
				Disconnect();
			}
		}

		private void Connect()
		{
			_connection = new SqlConnection(_connectionString.connectionString);
			_connection.Open();
		}

		public void RestoreDatabaseFromSnapshot()
		{
			// create snapshot if none exists? TODO

			try
			{
				var databaseName = _connectionString.databaseName;
				var databaseSnapshotName = String.Format("{0}_ss", databaseName);
				_logger.WriteEntry(String.Format("\nRestoring {0} from snapshot {1} ...", databaseName, databaseSnapshotName));

				Connect();

				var scriptToExecute = String.Format(
													"USE master;" +
													"RESTORE DATABASE {0} " +
													"FROM DATABASE_SNAPSHOT = '{1}';"
													, databaseName, databaseSnapshotName);

				using (var command = new SqlCommand(scriptToExecute, _connection))
					command.ExecuteNonQuery();

				_logger.WriteEntry(String.Format("Successfully restored {0} from snapshot {1}!", databaseName, databaseSnapshotName));

			}
			finally
			{
				Disconnect();
			}
		}

		private void Disconnect()
		{
			_connection.Close();
		}

	}

	internal class SqlDatabase
	{
		private readonly ConnectionString _connString;
		private readonly SqlConnection _sqlConnection;

		public SqlDatabase(ConnectionString a_ConnString, SqlConnection a_SqlConn)
		{
			_connString = a_ConnString;
			_sqlConnection = a_SqlConn;
		}

		public void Update(Script a_Script)
		{
			CustomizeQueryForDatabase(ref a_Script);

			var parsedScript = a_Script.Parse();
			foreach (var queryItem in parsedScript.Where(a_QueryItem => a_QueryItem.Trim().Length >= 5))
			{
				using (var cmd = new SqlCommand(queryItem, _sqlConnection))
				{
					cmd.ExecuteNonQuery();
				}
			}
		}

		private void CustomizeQueryForDatabase(ref Script a_Script)
		{
			a_Script.CustomizeQueryItem(Script.QueryItem.Database, _connString.databaseName);
			a_Script.CustomizeQueryItem(Script.QueryItem.Username, _connString.databaseUserId);
		}
	}

	internal class ConnectionString
	{
		public string serverName;
		public string databaseUserId;
		private string _databasePassword;
		public string databaseName;
		public string connectionString;

		public ConnectionString(string a_ConnectionString)
		{
			connectionString = a_ConnectionString;
			SetMembers();
		}

		private void SetMembers()
		{
			SetServerName();
			SetDatabaseUserId();
			SetDatabasePassword();
			SetDatabaseName();
		}

		private void SetServerName()
		{
			serverName = connectionString.Split(';')[0].Split('=')[1];
		}

		private void SetDatabaseUserId()
		{
			databaseUserId = connectionString.Split(';')[1].Split('=')[1];
		}

		private void SetDatabasePassword()
		{
			_databasePassword = connectionString.Split(';')[2].Split('=')[1];
		}

		private void SetDatabaseName()
		{
			databaseName = connectionString.Split(';')[3].Split('=')[1];
		}
	}
}
