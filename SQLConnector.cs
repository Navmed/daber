using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Daber
{
    class SQLConnector:IConnector
    {
        protected string connString;
        public SQLConnector(string connString)
        {
            this.connString = connString;
        }

        public DbConnection Connect()
        {
            return new SqlConnection(connString);
        }

		// Get the Primary Key/Identity Column
		public string GetIdentityColumn(string table)
		{
			// Todo: Add error handling
			DbConnection conn = Connect();
			conn.Open();
			DbCommand cmd = conn.CreateCommand();
			
			cmd.CommandText = string.Format(@"select c.name
											from sys.objects o 
											inner join sys.columns c on o.object_id = c.object_id
											where c.is_identity = 1
											AND o.name='{0}'", table);
			DbDataReader reader = cmd.ExecuteReader();
			if (reader != null)
			{
				while (reader.Read())
				{
					return reader.GetString(0);
				}
			}
			return null;
		}

        public int Insert(DbCommand cmd, string table, string cols, string vals, out long id)
        {
            cmd.CommandText = string.Format("INSERT INTO [{0}] ({1}) VALUES ({2})  SET @newId = SCOPE_IDENTITY()", table, cols, vals);

            SqlParameter newId = new SqlParameter("@newId", SqlDbType.BigInt);
            newId.Direction = ParameterDirection.Output;
            cmd.Parameters.Add(newId);

            int rows = cmd.ExecuteNonQuery();
            if (newId.Value is DBNull)
                id = 0;
            else
                id = Convert.ToInt64(newId.Value);

            return rows;
        }

        public void AddParameter(DbCommand dbCmd, string parName, object oVal)
        {
            SqlCommand cmd = (SqlCommand)dbCmd;

            if (oVal == null)
                cmd.Parameters.Add(parName, SqlDbType.VarChar).Value = DBNull.Value;
            else if (oVal.GetType() == typeof(string))
                cmd.Parameters.Add(parName, SqlDbType.VarChar).Value = oVal;
            else if (oVal.GetType() == typeof(int))
                cmd.Parameters.Add(parName, SqlDbType.Int).Value = oVal;
            else if (oVal.GetType() == typeof(ulong))
                cmd.Parameters.Add(parName, SqlDbType.BigInt).Value = oVal;
            else if (oVal.GetType() == typeof(float))
                cmd.Parameters.Add(parName, SqlDbType.Float).Value = oVal;
            else if (oVal.GetType() == typeof(double))
                cmd.Parameters.Add(parName, SqlDbType.Decimal).Value = oVal;
            else if (oVal.GetType() == typeof(DateTime))
                cmd.Parameters.Add(parName, SqlDbType.DateTime).Value = oVal;
            else if (oVal.GetType() == typeof(Int64))
                cmd.Parameters.Add(parName, SqlDbType.BigInt).Value = oVal;
            else if (oVal is bool)
                cmd.Parameters.Add(parName, SqlDbType.Bit).Value = Convert.ToBoolean(oVal);
            else if (oVal is byte)
                cmd.Parameters.Add(parName, SqlDbType.TinyInt).Value = Convert.ToByte(oVal);
            else if (oVal is Guid)
                cmd.Parameters.Add(parName, SqlDbType.UniqueIdentifier).Value = oVal;
            else if (oVal is short)
                cmd.Parameters.Add(parName, SqlDbType.SmallInt).Value = oVal;
            else if (oVal.GetType().IsEnum)
            {
                System.Type type = oVal.GetType();
                if (Enum.GetUnderlyingType(type) == typeof(Byte))
                    cmd.Parameters.Add(parName, SqlDbType.TinyInt).Value = Convert.ToByte(oVal);
                else if (Enum.GetUnderlyingType(type) == typeof(short))
                    cmd.Parameters.Add(parName, SqlDbType.SmallInt).Value = oVal;
                else if (Enum.GetUnderlyingType(type) == typeof(int))
                    cmd.Parameters.Add(parName, SqlDbType.Int).Value = oVal;
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
