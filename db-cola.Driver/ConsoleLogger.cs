using System;
using System.Collections;
using System.Threading;

namespace db_cola.Driver
{
    public class ConsoleLogger : ILogger
    {
        private static readonly Mutex _mutex = new Mutex();

        public void WriteEntry(ArrayList a_Entry)
        {
            _mutex.WaitOne();

            IEnumerator line = a_Entry.GetEnumerator();
            while (line.MoveNext())
                Console.WriteLine(line.Current);

            _mutex.ReleaseMutex();
        }

        public void WriteEntry(string a_Entry)
        {
            _mutex.WaitOne();

            Console.WriteLine(a_Entry);

            _mutex.ReleaseMutex();
        }
    }
}
