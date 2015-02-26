using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using mycargus.Core;

namespace dbcola
{
    public class DestinationSqlServer
    {
	public string Alias { get; set; }
	private ConnectionString _connectionString;
	private ILogger _logger;
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
		Connect();

		_logger.WriteEntry("\nExecuting scripts ...");


		SqlDatabase database = new SqlDatabase(_connectionString, _connection);
		foreach (Script script in a_Scripts)
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
		_logger.WriteEntry(String.Format("\nPreparing SQL scripts for execution on server {0} ...", Alias));
		_logger.WriteEntry(String.Format("on database {0} ...", _connectionString.databaseName));

		_connection = new SqlConnection(_connectionString.connectionString);
		_connection.Open();
	}

	private void Disconnect()
	{
		_connection.Close();
	}

    }

    internal class SqlDatabase
    {
	private ConnectionString _connString;
	private SqlConnection _sqlConnection;

	public SqlDatabase(ConnectionString a_ConnString, SqlConnection a_SqlConn)
	{
	    _connString = a_ConnString;
	    _sqlConnection = a_SqlConn;
	}

	public void Update(Script a_Script)
	{
	    CustomizeQueryForDatabase(ref a_Script);

	    string[] parsedScript = a_Script.Parse();
	    foreach (var queryItem in parsedScript)
	    {
		if (queryItem.Trim().Length < 5)
		    continue;

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
	private string databasePassword;
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
