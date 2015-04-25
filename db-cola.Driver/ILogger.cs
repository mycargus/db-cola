using System;
using System.Collections;

namespace db_cola.Driver
{
    public interface ILogger
    {
        void WriteEntry(ArrayList a_Entry);
        void WriteEntry(String a_Entry);
    }
}
