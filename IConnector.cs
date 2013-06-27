using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Daber
{
    public interface IConnector
    {
        DbConnection Connect();
        void AddParameter(DbCommand cmd, string parName, object oVal);
        int Insert(DbCommand cmd, string table, string cols, string vals, out long id);
    }
}
