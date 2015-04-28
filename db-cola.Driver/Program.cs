using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace db_cola.Driver
{
	class Program
	{
		const string _FilesToOmit = "eBankHolidays.sql,eRegionZipCode.sql,eUSStates.sql,pESignatureFiles_Get.sql";
		const string _TablesDirectory = "Tables";
		private static List<Script> _sqlScriptsToRun;

		public enum LoggerType
		{
			FileLogger = 1,
			ConsoleLogger = 2
		}

		static void Main(string[] a_Args)
		{
			var logger = GetLogger(a_Args);

			try
			{
				// Add all db modification scripts for Simple Interest Loan product
				_sqlScriptsToRun = new List<Script>();
				var scriptDirectoryPath = a_Args[1];
				if (scriptDirectoryPath.Split('\\').Last().Equals(_TablesDirectory, StringComparison.CurrentCultureIgnoreCase))
				{
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE TABLE SimpleInterestLoan.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE TABLE Transaction.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_LoanApproval.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_ProductType.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_ProductType.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TABLE_BrokerProduct.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE_TransactionPendingLateCharges.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\CREATE-INIT TABLE TransactionType.sql"));

					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER_SystemDefaults.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER TABLE PendingAppsLoans.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER_BrokerDocs_Add_SimpleInterestDocs.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER TABLE ESignatures_Add_SignedEFT.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER TABLES Trans4 Trans5 ACHTransactions - Transaction.sql"));
					//_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\ALTER TABLE ScheduledTransactions - Chargeback Probability.sql"));
					//_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT SOEConstants - Chargeback Probability.sql"));
					//_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\.sql"));

					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_TextTemplateTypes.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT INTO TextTemplates.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_BrokerProduct.sql"));

					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_Brokers_MRS.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_SysDef_MRS.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_Integration_MRS.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_Regions_MRS.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_IncomeToPrincipalSubRules_MRS.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_IncRules_MRS.sql"));
					_sqlScriptsToRun.Add(new Script(scriptDirectoryPath + "\\INSERT_INTO_VIPConfig_MRS.sql"));
				}

				ValidateArguments(a_Args);

				FilterScriptFiles(a_Args);

				var sqlServers = GetDestinationSqlServers(a_Args[0].Trim(), logger);
				foreach (var server in sqlServers)
					server.UpdateDatabase(_sqlScriptsToRun);

				logger.WriteEntry(Environment.NewLine);
				logger.WriteEntry("\nDone!");
				logger.WriteEntry(Environment.NewLine);
			}
			catch (Exception ex)
			{
				PrintErrorMessage(ex.Message + Environment.NewLine + ex.StackTrace, logger);
				Console.WriteLine("\nPress enter to continue, or close the console window to quit.");
				Console.ReadLine();
				Environment.Exit(1);
			}
		}

		static void FilterScriptFiles(IReadOnlyList<string> a_ProgramArgs)
		{
			var onlyRunSpecifiedScripts = a_ProgramArgs[1].Split('\\').Last().Equals(_TablesDirectory, StringComparison.CurrentCultureIgnoreCase);
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
			return a_Args != null && a_Args.Count == 2;
		}

		static ILogger GetLogger(IReadOnlyList<string> a_Args)
		{
			ILogger logger;
			//var loggerType = Convert.ToInt32(a_Args[2]);
			switch (0)
			{
				case (int)LoggerType.FileLogger:
					logger = new FileLogger(String.Format("{0}\\{1}", Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "dbUpdateLog.txt"));
					break;
				case (int)LoggerType.ConsoleLogger:
					logger = new ConsoleLogger();
					break;
				default:
					logger = new ConsoleLogger();
					break;
			}
			return logger;
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
