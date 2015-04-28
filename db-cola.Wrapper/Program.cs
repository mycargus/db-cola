using System;
using System.Collections.Generic;
using System.Diagnostics;
using db_cola.Driver;

namespace db_cola.Wrapper
{
	class Program
	{
		const string _ProgramToRun = "..db-cola\\bin\\Debug\\db-cola.exe";
		private static readonly ILogger Logger = new ConsoleLogger();

		static void Main(string[] a_Args)
		{

			// specify the databases you want to update (you may specify more than one) *** TODO before running program!
			const string YourDatabase = "server=your_database_server;uid=your_username;pwd=your_password;database=your_database;";
			const string YourOtherDatabase = "server=your_other_database_server;uid=your_username;pwd=your_password;database=your_other_database;";

			var databases = new[]
			{
				YourDatabase,
				YourOtherDatabase
			};

			// specify the directories that contain the sql scripts you want to execute *** TODO before running program!
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

			var exitCode = 0; // no error
			try
			{
				ExecuteDbCola(databases, scriptDirectories);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message + Environment.NewLine + e.StackTrace);
				Console.WriteLine("\nPress enter to exit");
				Console.ReadLine();
				exitCode = 1;
			}
			finally
			{
				Environment.Exit(exitCode);
			}
		}

		private static void ExecuteDbCola(IEnumerable<string> a_ConnectionString, string[] a_ScriptDirectories)
		{
			if (a_ConnectionString == null || a_ScriptDirectories == null) throw new ArgumentNullException();

			foreach (var connectionString in a_ConnectionString)
			{
				var destinationDatabaseServer = new DestinationSqlServer(connectionString, Logger);

				try
				{
					// kill any current connections to the database to prevent errors
					destinationDatabaseServer.DisconnectAllOtherUsers();

					// create snapshot just in case something goes wrong during configuration
					if (!destinationDatabaseServer.HasSnapshot())
						destinationDatabaseServer.CreateDatabaseSnapshot();

					// configure personal database copy for testing
					destinationDatabaseServer.ConfigurePersonalDatabase();

					// execute scripts as defined above in Main() 
					foreach (var directory in a_ScriptDirectories)
						ExecuteDbCola(connectionString, directory);

					// drop the current ss
					destinationDatabaseServer.DropDatabaseSnapshot();

					// create new ss now that we're all done
					destinationDatabaseServer.CreateDatabaseSnapshot();

					Logger.WriteEntry("\nAll Done!");
				}
				finally
				{
					// just in case something went wrong during configuration
					if (destinationDatabaseServer.HasSnapshot())
						destinationDatabaseServer.RestoreDatabaseFromSnapshot();

					Console.WriteLine("\nPress enter to continue");
					Console.ReadLine();
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
