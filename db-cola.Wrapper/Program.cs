using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace db_cola.Wrapper
{
	class Program
	{
		const string _ProgramToRun = "..db-cola\\bin\\Debug\\db-cola.exe";

		static void Main(string[] a_Args)
		{

			// specify the databases you want to update (you may specify more than one)
			const string YourDatabase = "server=your_database_server;uid=your_username;pwd=your_password;database=your_database;";
			const string YourOtherDatabase = "server=your_other_database_server;uid=your_username;pwd=your_password;database=your_other_database;";

			var databases = new[]
			{
				YourDatabase,
				YourOtherDatabase
			};

			// specify the directories that contain the sql scripts you want to execute
			const string TablesDirectory = "\"C:\\Source Code\\Database\\Sql\\Tables\"";
			const string ProcsDirectory = "\"C:\\Source Code\\Database\\Sql\\Procs\"";
			const string JobsDirectory = "\"C:\\Source Code\\Database\\Sql\\Jobs\"";
			const string FunctionsDirectory = "\"C:\\Source Code\\Database\\Sql\\Functions\"";
			
			var scriptDirectories = new[]
			{
				TablesDirectory, 
				ProcsDirectory,
				FunctionsDirectory
			};

			try
			{
				ExecuteDbCola(databases, scriptDirectories);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
			}
			finally
			{
				Console.ReadLine();
				Environment.Exit(1);
			}
		}

		private static void ExecuteDbCola(IEnumerable<string> a_ConnectionString, string[] a_ScriptDirectories)
		{
			if (a_ConnectionString == null || a_ScriptDirectories == null) throw new ArgumentNullException();

			foreach (var connectionString in a_ConnectionString)
			{
				foreach (var directory in a_ScriptDirectories)
				{
					ExecuteDbCola(connectionString, directory);
				}
			}
		}

		private static void ExecuteDbCola(string a_ConnectionString, string a_ScriptDirectory)
		{
			var args = new[]
			{
				a_ConnectionString,
				a_ScriptDirectory
			};

			var programArguments = String.Join(" ", args);
			var dbcola = new Process();

			try
			{
				dbcola.StartInfo.FileName = _ProgramToRun;
				dbcola.StartInfo.Arguments = programArguments;
				dbcola.StartInfo.UseShellExecute = false;
				dbcola.Start();
			}
			finally
			{
				dbcola.WaitForExit();
				dbcola.Close();
				dbcola.Dispose();
			}
		}
	}
}
