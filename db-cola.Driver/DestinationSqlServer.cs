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

		public bool HasSnapshot()
		{
			var hasSnapshot = false;
			try
			{
				Connect();

				hasSnapshot = SnapshotAlreadyExists();
			}
			finally
			{
				Disconnect();
			}

			return hasSnapshot;
		}

		private bool SnapshotAlreadyExists()
		{
			var snapshotAlreadyExists = false;
			var databaseSnapshotName = String.Format("{0}_ss", _connectionString.databaseName);

			var scriptToExecute = String.Format("SELECT ISNULL(DB_ID('{0}'), 0);"
												, databaseSnapshotName);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				snapshotAlreadyExists = (Convert.ToInt32(command.ExecuteScalar()) > 0);

			return snapshotAlreadyExists;
		}

		public void UpdateDatabase(List<Script> a_Scripts)
		{
			try
			{
				Connect();

				UpdateDatabaseWithScripts(a_Scripts);
			}
			finally
			{
				Disconnect();
			}
		}

		private void UpdateDatabaseWithScripts(IEnumerable<Script> a_Scripts)
		{
			_logger.WriteEntry(String.Format("\nPreparing SQL scripts for execution on server {0} ...", Alias));
			_logger.WriteEntry(String.Format("on database {0} ...", _connectionString.databaseName));
			_logger.WriteEntry("\nExecuting scripts ...");


			var database = new SqlDatabase(_connectionString, _connection);
			foreach (var script in a_Scripts)
			{
				_logger.WriteEntry(script.GetFileName());
				database.Update(script);
			}

		}

		private void Connect()
		{
			_connection = new SqlConnection(_connectionString.connectionString);
			_connection.Open();
		}

		public void ConfigurePersonalDatabase()
		{
			try
			{
				Connect();

				ConfigurePersonalDatabase(_connectionString.databaseName, _connectionString.databaseUserId, _connectionString.databasePassword);
			}
			finally
			{
				Disconnect();
			}
		}

		private void ConfigurePersonalDatabase(string a_DatabaseName, string a_UserId, string a_Password)
		{

			_logger.WriteEntry(String.Format("\nConfiguring personal database {0} ...", a_DatabaseName));

			AddUserLogin(a_DatabaseName, a_UserId, a_Password);
			DropUnnecessaryUsers(a_DatabaseName, a_UserId);
			AddDatabaseUser(a_DatabaseName, a_UserId);

			_logger.WriteEntry(String.Format("Successfully configured personal database {0} !", a_DatabaseName));
		}

		private void AddUserLogin(string a_DatabaseName, string a_UserId, string a_Password)
		{
			var scriptToExecute = String.Format(
												"USE [master];" +
												"IF NOT EXISTS (SELECT name FROM master.sys.server_principals WHERE name = '{0}')\n" +
												"BEGIN\n" +
												"CREATE LOGIN {0}\n" +
													"WITH PASSWORD = '{1}',\n" +
													"DEFAULT_DATABASE = {2},\n" +
													"DEFAULT_LANGUAGE = [us_english],\n" +
													"CHECK_EXPIRATION = OFF,\n" +
													"CHECK_POLICY = ON;\n" +
												"END\n" +
												"EXEC sp_addsrvrolemember @loginame = {0}, @rolename = N'sysadmin';"
												, a_UserId, a_Password, a_DatabaseName);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();
		}

		private void DropUnnecessaryUsers(string a_DatabaseName, string a_UserId)
		{
			var scriptToExecute = String.Format(
												"USE [{0}];" +
												"IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'BrkWebUser')\n" +
												"DROP USER [BrkWebUser];\n" +
												"IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'BrkProgramUser')\n" +
												"DROP USER [BrkProgramUser];\n" +
												"IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'{1}')\n" +
												"DROP USER [{1}];\n"
												, a_DatabaseName, a_UserId);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();
		}

		private void AddDatabaseUser(string a_DatabaseName, string a_UserId)
		{
			var scriptToExecute = String.Format(
												"USE [{0}];" +
												"CREATE USER {1} FOR LOGIN {1};\n" +
												"ALTER ROLE [db_owner] ADD MEMBER {1};\n" +
												"GRANT EXECUTE TO {1};\n" +
												"GRANT INSERT TO {1};\n" +
												"GRANT SELECT TO {1};\n" +
												"GRANT UPDATE TO {1};\n" +
												"GRANT DELETE TO {1};\n"
												, a_DatabaseName, a_UserId);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();
		}

		public void RestoreDatabaseFromSnapshot()
		{
			try
			{
				Connect();

				RestoreDatabaseFromSnapshot(_connectionString.databaseName);
			}
			finally
			{
				Disconnect();
			}
		}

		private void RestoreDatabaseFromSnapshot(string a_DatabaseName)
		{
			var databaseSnapshotName = String.Format("{0}_ss", a_DatabaseName);
			_logger.WriteEntry(String.Format("\nSomething bad happened ... Restoring database {0} from initial snapshot {1} ...", a_DatabaseName, databaseSnapshotName));

			var scriptToExecute = String.Format(
												"USE master;" +
												"RESTORE DATABASE {0} " +
												"FROM DATABASE_SNAPSHOT = '{1}';"
												, a_DatabaseName, databaseSnapshotName);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();

			_logger.WriteEntry(String.Format("Database {0} restored successfully.", a_DatabaseName));
		}

		public void CreateDatabaseSnapshot()
		{
			try
			{
				Connect();

				CreateDatabaseSnapshot(_connectionString.databaseName);
			}
			finally
			{
				Disconnect();
			}
		}

		private void CreateDatabaseSnapshot(string a_DatabaseName)
		{
			var databaseSnapshotName = String.Format("{0}_ss", a_DatabaseName);
			_logger.WriteEntry(String.Format("\nCreating snapshot {0} of database {1} ...", databaseSnapshotName, a_DatabaseName));

			var scriptToExecute = String.Format(
												"USE master;\n" +
												"CREATE DATABASE {0} ON\n" +
												"(\n" +
													"NAME = Broker_DB,\n" +
													"FILENAME = 'E:\\MSSQL_DATA\\Snapshots\\{1}_data.ss'" +
												")\n" +
												"AS SNAPSHOT OF {1};"
												, databaseSnapshotName, a_DatabaseName);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();

			_logger.WriteEntry("Success! Moving on ...");
		}

		public void DropDatabaseSnapshot()
		{
			try
			{
				Connect();

				DropDatabaseSnapshot(_connectionString.databaseName);
			}
			finally
			{
				Disconnect();
			}
		}

		private void DropDatabaseSnapshot(string a_DatabaseName)
		{
			var databaseSnapshotName = String.Format("{0}_ss", a_DatabaseName);
			_logger.WriteEntry(String.Format("\nDropping snapshot {0} of database {1} ...", databaseSnapshotName, a_DatabaseName));

			var scriptToExecute = String.Format(
												"USE master;\n" +
												"DROP DATABASE {0};\n"
												, databaseSnapshotName);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();

			_logger.WriteEntry("Success! Moving on ...");
		}

		public void DisconnectAllOtherUsers()
		{
			try
			{
				Connect();

				DisconnectAllOtherUsers(_connectionString.databaseName);
			}
			finally
			{
				Disconnect();
			}
		}

		private void DisconnectAllOtherUsers(string a_DatabaseName)
		{
			_logger.WriteEntry(String.Format("\nDisconnecting any current connections to database {0} ...", a_DatabaseName));

			var scriptToExecute = String.Format(
												"USE master;\n" +
												"DECLARE @kill varchar(8000) = '';\n" +
												"SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'\n" +
												"FROM master..sysprocesses\n" +
												"WHERE dbid = DB_ID('{0}')\n" +
												"EXEC(@kill);\n"
												, a_DatabaseName);

			using (var command = new SqlCommand(scriptToExecute, _connection))
				command.ExecuteNonQuery();

			_logger.WriteEntry("Success! Moving on ...");
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
		public string databasePassword;
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
			databasePassword = connectionString.Split(';')[2].Split('=')[1];
		}

		private void SetDatabaseName()
		{
			databaseName = connectionString.Split(';')[3].Split('=')[1];
		}
	}
}
