using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using mycargus.Core;

namespace dbcola
{
    class Program
    {
	const string FILES_TO_OMIT = "eBankHolidays.sql,eRegionZipCode.sql,eUSStates.sql";
	const string TABLES_DIRECTORY = "C:\\Source Code\\Web Applications\\TPLNetwork\\Sql\\Tables";
	const string PROCS_DIRECTORY = "C:\\Source Code\\Web Applications\\TPLNetwork\\Sql\\Procs";

	private const string DATABASE_CONNECTION_STRING = "server=your.dbserver.com;uid=username;pwd=password;database=your_db_name;";
	private static List<Script> _sqlScriptsToRun;

	static void Main(string[] args)
	{
	    args = new[]
		    {
			    // destination sql servers' connection strings, pipe-delimited |
			    DATABASE_CONNECTION_STRING,

			    // full path to sql script directory
			    PROCS_DIRECTORY//,	    
 
			    // (optional) script files' last modified date (yyyy-mm-dd). Will run all scripts in directory if left blank.
			    //"2015-02-06"							     
		    };

	    var scriptDirectoryPath = args[1];
	    _sqlScriptsToRun = new List<Script>();
	    if (scriptDirectoryPath.Split('\\').Last().Equals("Tables", StringComparison.CurrentCultureIgnoreCase))
	    {
		int priority = 0;
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_ProductType.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_BrokerProduct.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_LoanSequence.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_LoanApproval.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_ProductType.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_BrokerProduct.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_Brokers_MSW.sql", ++priority));
		_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER TABLE PendingAppsLoans.sql", ++priority));
	    }


	    //ILogger logger = new FileLogger("C:/Users/Michael Hargiss/Desktop/dbUpdateLog.txt", false);
	    ILogger logger = new ConsoleLogger();

	    try
	    {
		ValidateArguments(args);

		FilterScriptFiles(args);

		var sqlServers = GetDestinationSqlServers(args[0].Trim(), logger);
		foreach (DestinationSqlServer server in sqlServers)
		{
		    server.UpdateDatabase(_sqlScriptsToRun);
		}
	    }
	    catch (Exception ex)
	    {
		PrintErrorMessage(ex.Message + Environment.NewLine + ex.StackTrace, logger);
	    }
	    finally
	    {
		if (logger.GetType() == typeof(ConsoleLogger))
		{
		    logger.WriteEntry("\nDone!");
		    Console.ReadLine();
		}
	    }
	}

	static void FilterScriptFiles(string[] a_ProgramArgs)
	{
	    DateTime fileLastModifiedDate = GetFilesLastModifiedDate(a_ProgramArgs);

	    int orderNumber = _sqlScriptsToRun.Count;
	    foreach (string filePath in GetSqlScriptFileNames(a_ProgramArgs[1].Trim()))
	    {
		var fileInfo = new FileInfo(filePath);
		if ((fileInfo.LastWriteTime < fileLastModifiedDate) || (OmitFile(fileInfo.Name)))
		    continue;

		Script script = new Script(filePath, orderNumber);
		
		var match = _sqlScriptsToRun.FirstOrDefault(s => s.GetFileName().Equals(script.GetFileName()));
		if (match != null)
		    continue;

		script.UpdateOrderNumber(++orderNumber);
		_sqlScriptsToRun.Add(script);
	    }
	}

	static bool OmitFile(string a_FileName)
	{
	    return FILES_TO_OMIT.Contains(a_FileName);
	}

	static List<string> GetSqlScriptFileNames(string a_ScriptsDirectoryPath)
	{
		if (!Directory.Exists(a_ScriptsDirectoryPath) || Directory.GetFiles(a_ScriptsDirectoryPath).Length == 0)
			throw new Exception(String.Format("Directory '{0}' doesn't exist or is empty!", a_ScriptsDirectoryPath));

		return Directory.GetFiles(a_ScriptsDirectoryPath, "*.SQL", SearchOption.AllDirectories).ToList();
	}

	static DateTime GetFilesLastModifiedDate(string[] args)
	{
	    string date = String.Empty;
	    if (args.Length > 2)
		date = args[2].Trim();

	    return (date != String.Empty && date.Length > 0) 
		? DateTime.Parse(date)
		: DateTime.MinValue;
	}

	static List<DestinationSqlServer> GetDestinationSqlServers(string a_ConnectionStrings, ILogger a_Logger)
	{
	    var destinationSqlServers = new List<DestinationSqlServer>();

	    foreach (string connString in GetConnectionStrings(a_ConnectionStrings))
	    {
		destinationSqlServers.Add(new DestinationSqlServer(connString, a_Logger));
	    }

	    return destinationSqlServers;
	}

	static List<string> GetConnectionStrings(string a_ConnectionStrings)
	{
	    try
	    {
		List<string> parsedConnStrings = new List<string>();
		if (a_ConnectionStrings.Contains('|'))
		{
		    foreach (string connString in a_ConnectionStrings.Split('|'))
		    {
			if (connString.Trim().Length > 0)
			    parsedConnStrings.Add(connString);
		    }
		}
		else
		{
			parsedConnStrings.Add(a_ConnectionStrings);
		}
		return parsedConnStrings;
	    }
	    catch (Exception ex)
	    {
		throw new Exception("Unable to parse the supplied arguments! " + ex.Message);
	    }
	}

	static void ValidateArguments(string[] args)
	{
	    if (!HasCorrectNumberOfArgs(args))
	    {
		Console.WriteLine("\n\n" + GetHelp());
		throw new ArgumentException("All required arguments weren't supplied!\n\n" + GetHelp());
	    }
	}

	static bool HasCorrectNumberOfArgs(string[] args)
	{
	    return args.Length >= 2 && args.Length <= 4;
	}

	static void PrintErrorMessage(string a_Message, ILogger a_Logger)
	{
	    a_Logger.WriteEntry(Environment.NewLine);
	    a_Logger.WriteEntry(a_Message);
	    a_Logger.WriteEntry(Environment.NewLine);
	}

	static string GetHelp()
	{
	    return "Expected Arguments:" +
		Environment.NewLine +
		"1: Pipe-delimited ('|') list of destination servers' connection strings" +
		Environment.NewLine +
		"2: Full path to SQL scripts directory" +
		Environment.NewLine +
		"3 (optional): Script files' last modified date (yyyy-mm-dd).  Default will run all SQL scripts in the supplied directory." +
		Environment.NewLine;
	}

	static void AskUserWhatToDo(Exception ex)
	{
	    string instructions = "\nHit enter to continue, or type 'n' and hit enter to exit program.";
	    Console.WriteLine("\nBummer. That one didn't work...  " + ex.Message);
	    Console.WriteLine(instructions);
	    string response = Console.ReadLine();
	    do
	    {
		if (response == null)
		{
		    Console.WriteLine(instructions);
		    response = Console.ReadLine();
		}
		else if (response.Equals(""))
		{
		    break;
		}
		else if (response.Equals("n"))
		{
		    Console.WriteLine("\nClosing program...");
		    throw new ApplicationException("User told me to close.");
		}
		else
		{
		    Console.WriteLine(instructions);
		    response = Console.ReadLine();
		}
	    } while (response == null || !response.Equals("") || !response.Equals("n"));
	}

    }
}
