using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient;

namespace Daber
{
    class MySQLConnector:IConnector
    {
        protected string connString;
        public MySQLConnector(string connString)
        {
            this.connString = connString;
        }

        public DbConnection Connect(string connString)
        {
            return new MySqlConnection(connString);
        }
        
        public void AddParameter(DbCommand dbCmd, string parName, object oVal)
        {
            MySqlCommand cmd = (MySqlCommand)dbCmd;

            if (oVal == null)
                cmd.Parameters.Add(parName, MySqlDbType.VarChar).Value = DBNull.Value;
            else if (oVal.GetType() == typeof(string))
                cmd.Parameters.Add(parName, MySqlDbType.VarChar).Value = oVal;
            else if (oVal.GetType() == typeof(int))
                cmd.Parameters.Add(parName, MySqlDbType.Int32).Value = oVal;
            else if (oVal.GetType() == typeof(UInt32))
                cmd.Parameters.Add(parName, MySqlDbType.UInt32).Value = oVal;
            else if (oVal.GetType() == typeof(ulong))
                cmd.Parameters.Add(parName, MySqlDbType.Int64).Value = oVal;
            else if (oVal.GetType() == typeof(float))
                cmd.Parameters.Add(parName, MySqlDbType.Float).Value = oVal;
            else if (oVal.GetType() == typeof(double))
                cmd.Parameters.Add(parName, MySqlDbType.Decimal).Value = oVal;
            else if (oVal.GetType() == typeof(DateTime))
                cmd.Parameters.Add(parName, MySqlDbType.DateTime).Value = oVal;
            else if (oVal.GetType() == typeof(Int64))
                cmd.Parameters.Add(parName, MySqlDbType.Int64).Value = oVal;
            else if (oVal is bool)
                cmd.Parameters.Add(parName, MySqlDbType.Bit).Value = Convert.ToBoolean(oVal);
            else if (oVal is byte)
                cmd.Parameters.Add(parName, MySqlDbType.Byte).Value = Convert.ToByte(oVal);
            else if (oVal is Guid)
                cmd.Parameters.Add(parName, MySqlDbType.Guid).Value = oVal;
            else if (oVal is short)
                cmd.Parameters.Add(parName, MySqlDbType.Int16).Value = oVal;
            else if (oVal.GetType().IsEnum)
            {
                System.Type type = oVal.GetType();
                if (Enum.GetUnderlyingType(type) == typeof(Byte))
                    cmd.Parameters.Add(parName, MySqlDbType.Byte).Value = Convert.ToByte(oVal);
                else if (Enum.GetUnderlyingType(type) == typeof(short))
                    cmd.Parameters.Add(parName, MySqlDbType.Int16).Value = oVal;
                else if (Enum.GetUnderlyingType(type) == typeof(int))
                    cmd.Parameters.Add(parName, MySqlDbType.Int32).Value = oVal;
                else
                    throw new Exception("Cannot handle " + oVal.GetType().ToString() + " in addParameter");
            }
            else
            {
                throw new Exception("Cannot handle " + oVal.GetType().ToString() + " in addParameter");
            }
        }
    }
}
