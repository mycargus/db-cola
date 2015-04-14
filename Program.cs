using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using mycargus.Core;

namespace dbcola
{
    class Program
    {
		const string _FilesToOmit = "pSomeProcYouWantToIgnore.sql,pAnotherProcToIgnore.sql";
		const string _TablesDirectory = "C:\\Database\\Sql\\Tables";


		private const string _YourDatabases = "server=test.databaseUri.com;uid=your_username;pwd=your_password;database=your_database;|server=test.otherDatabaseUri.com;uid=your_username;pwd=your_password;database=your_other_database;";
		private static List<Script> _sqlScriptsToRun;

		static void Main(string[] a_Args)
		{
			a_Args = new[]
				{
					// destination sql servers' connection strings, pipe-delimited |
					_YourDatabases,

					// full path to sql script directory
					_TablesDirectory//,	    
 
					// (optional) script files' last modified date (yyyy-mm-dd). Will run all scripts in directory if left blank.
					//"2015-02-06"							     
				};


			var scriptDirectoryPath = a_Args[1];
			_sqlScriptsToRun = new List<Script>();
			if (scriptDirectoryPath.Split('\\').Last().Equals("Tables", StringComparison.CurrentCultureIgnoreCase))
			{
				_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_Script.sql"));
				_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_Table_Script.sql"));
			}


			//ILogger logger = new FileLogger("./dbUpdateLog.txt", false);
			ILogger logger = new ConsoleLogger();

			try
			{
				ValidateArguments(a_Args);

				FilterScriptFiles(a_Args);

				var sqlServers = GetDestinationSqlServers(a_Args[0].Trim(), logger);
				foreach (var server in sqlServers)
					server.UpdateDatabase(_sqlScriptsToRun);
		    
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

		static void FilterScriptFiles(IReadOnlyList<string> a_ProgramArgs)
		{
			var onlyRunSpecifiedScripts = a_ProgramArgs[1].Equals(_TablesDirectory, StringComparison.CurrentCultureIgnoreCase);
			if (onlyRunSpecifiedScripts) return;

			var fileLastModifiedDate = GetFilesLastModifiedDate(a_ProgramArgs);

			foreach (var filePath in GetSqlScriptFileNames(a_ProgramArgs[1].Trim()))
			{
				var fileInfo = new FileInfo(filePath);
				if ((fileInfo.LastWriteTime < fileLastModifiedDate) || (OmitFile(fileInfo.Name)))
					continue;

				var script = new Script(filePath);
		
				var match = _sqlScriptsToRun.FirstOrDefault(a_S => a_S.GetFileName().Equals(script.GetFileName()));
				if (match != null)
					continue;

				_sqlScriptsToRun.Add(script);
			}
		}

		static bool OmitFile(string a_FileName)
		{
			return _FilesToOmit.Contains(a_FileName);
		}

		static IEnumerable<string> GetSqlScriptFileNames(string a_ScriptsDirectoryPath)
		{
			if (!Directory.Exists(a_ScriptsDirectoryPath) || Directory.GetFiles(a_ScriptsDirectoryPath).Length == 0)
				throw new Exception(String.Format("Directory '{0}' doesn't exist or is empty!", a_ScriptsDirectoryPath));

			return Directory.GetFiles(a_ScriptsDirectoryPath, "*.SQL", SearchOption.AllDirectories).ToList();
		}

		static DateTime GetFilesLastModifiedDate(IReadOnlyList<string> a_Args)
		{
			var date = String.Empty;
			if (a_Args.Count > 2)
			date = a_Args[2].Trim();

			return (date != String.Empty && date.Length > 0) 
			? DateTime.Parse(date)
			: DateTime.MinValue;
		}

		static IEnumerable<DestinationSqlServer> GetDestinationSqlServers(string a_ConnectionStrings, ILogger a_Logger)
		{
			return GetConnectionStrings(a_ConnectionStrings).Select(a_ConnString => new DestinationSqlServer(a_ConnString, a_Logger)).ToList();
		}

		static IEnumerable<string> GetConnectionStrings(string a_ConnectionStrings)
		{
			try
			{
				var parsedConnStrings = new List<string>();
				if (a_ConnectionStrings.Contains('|'))
					parsedConnStrings.AddRange(a_ConnectionStrings.Split('|').Where(a_ConnString => a_ConnString.Trim().Length > 0));
				else
					parsedConnStrings.Add(a_ConnectionStrings);
				return parsedConnStrings;
			}
			catch (Exception ex)
			{
				throw new Exception("Unable to parse the supplied arguments! " + ex.Message);
			}
		}

		static void ValidateArguments(IReadOnlyCollection<string> a_Args)
		{
			if (HasCorrectNumberOfArgs(a_Args)) return;
			Console.WriteLine("\n\n" + GetHelp());
			throw new ArgumentException("All required arguments weren't supplied!\n\n" + GetHelp());
		}

		static bool HasCorrectNumberOfArgs(IReadOnlyCollection<string> a_Args)
		{
			return a_Args.Count >= 2 && a_Args.Count <= 4;
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

		static void AskUserWhatToDo(Exception a_Ex)
		{
			Console.WriteLine("\nBummer. That one didn't work...  " + a_Ex.Message);
			const string Instructions = "\nHit enter to continue, or type 'n' and hit enter to exit program.";
			Console.WriteLine(Instructions);
			var response = Console.ReadLine();
			do
			{
				if (response == null)
				{
					Console.WriteLine(Instructions);
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
					Console.WriteLine(Instructions);
					response = Console.ReadLine();
				}
			} while (response == null || !response.Equals("") || !response.Equals("n"));
		}

    }
}
