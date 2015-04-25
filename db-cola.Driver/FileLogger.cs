using System;
using System.Collections;
using System.IO;
using System.Threading;

namespace db_cola.Driver
{
    public class FileLogger : ILogger
    {
        private static readonly Mutex _mutex = new Mutex();
        private readonly StreamWriter _output;

        public FileLogger(String a_FileName, bool a_AppendToLog = true)
        {
            _output = new StreamWriter(a_FileName, a_AppendToLog);
        }

        public void WriteEntry(ArrayList a_Entry)
        {
            _mutex.WaitOne();

            IEnumerator line = a_Entry.GetEnumerator();
            while (line.MoveNext())
                _output.WriteLine(line.Current);
            _output.Flush();

            _mutex.ReleaseMutex();
        }

        public void WriteEntry(string a_Entry)
        {
            _mutex.WaitOne();

            _output.WriteLine(a_Entry);
            _output.Flush();

            _mutex.ReleaseMutex();
        }
    }
}
