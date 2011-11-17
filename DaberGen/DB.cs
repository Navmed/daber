using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Text.RegularExpressions;

using System.Data.Common;
using MySql.Data.MySqlClient;


namespace DaberGen
{
	public enum EClassType
	{
		BOOL, INT, STRING, DOUBLE, LONG
	}

	public enum EDatabase
	{
		NotSpecified = 0,
		SQLServer = 1,
		MySQL = 2
	}


	class DB
	{
		public class Field
		{
			public string classname;
			public string dbname;
			Type classtype;
			public int datatype;
		}

		Dictionary<int, string> map = new Dictionary<int, string>();
		

		EDatabase database = EDatabase.NotSpecified;

		protected string connString;
		public string ConnString { get { return (this.connString); } set { this.connString = value; } }

		

		public DB(string dbType, string connString)
		{
			this.connString = connString;


			if (dbType.StartsWith("my"))
			{
				database = EDatabase.MySQL;

				map.Add((int)MySqlDbType.Decimal, "double");
				map.Add((int)MySqlDbType.Byte, "byte");
				map.Add((int)MySqlDbType.Int16, "short");
				map.Add((int)MySqlDbType.Int32, "int");
				map.Add((int)MySqlDbType.Float, "float");
				map.Add((int)MySqlDbType.Double, "double");
				map.Add((int)MySqlDbType.Timestamp, "DateTime");
				map.Add((int)MySqlDbType.Int64, "long");
				map.Add((int)MySqlDbType.Int24, "int");
				map.Add((int)MySqlDbType.Date, "DateTime");
				map.Add((int)MySqlDbType.Time, "DateTime");
				map.Add((int)MySqlDbType.DateTime, "DateTime");
				map.Add((int)MySqlDbType.Year, "DateTime");
				map.Add((int)MySqlDbType.Newdate, "DateTime");
				map.Add((int)MySqlDbType.VarString, "string");
				map.Add((int)MySqlDbType.Bit, "bool");
				map.Add((int)MySqlDbType.NewDecimal, "double");
				map.Add((int)MySqlDbType.Enum, "int");
				
				map.Add((int)MySqlDbType.TinyBlob, "byte[]");
				map.Add((int)MySqlDbType.MediumBlob, "byte[]");
				map.Add((int)MySqlDbType.LongBlob, "byte[]");
				map.Add((int)MySqlDbType.Blob, "byte[]");
				map.Add((int)MySqlDbType.VarChar, "string");
				map.Add((int)MySqlDbType.String, "string");
				
				map.Add((int)MySqlDbType.UByte, "byte");
				map.Add((int)MySqlDbType.UInt16, "UInt16");
				map.Add((int)MySqlDbType.UInt32, "UInt32");
				map.Add((int)MySqlDbType.UInt64, "UInt64");
				map.Add((int)MySqlDbType.UInt24, "UInt32");
				map.Add((int)MySqlDbType.Binary, "byte[]");
				map.Add((int)MySqlDbType.VarBinary, "byte[]");
				map.Add((int)MySqlDbType.TinyText, "string");
				map.Add((int)MySqlDbType.MediumText, "string");
				map.Add((int)MySqlDbType.LongText, "string");
				map.Add((int)MySqlDbType.Text, "string");
				map.Add((int)MySqlDbType.Guid, "Guid");

			}
			else
			{
				database = EDatabase.SQLServer;

				map.Add((int)SqlDbType.BigInt, "long");
				map.Add((int)SqlDbType.Bit, "bool");
				map.Add((int)SqlDbType.Char, "string");
				map.Add((int)SqlDbType.DateTime, "DateTime");
				map.Add((int)SqlDbType.Decimal, "double");
				map.Add((int)SqlDbType.Float, "float");
				map.Add((int)SqlDbType.Int, "int");
				map.Add((int)SqlDbType.NChar, "string");
				map.Add((int)SqlDbType.NText, "string");
				map.Add((int)SqlDbType.NVarChar, "string");
				map.Add((int)SqlDbType.SmallInt, "short");
				map.Add((int)SqlDbType.Text, "string");
				map.Add((int)SqlDbType.VarChar, "string");
				map.Add((int)SqlDbType.Image, "byte[]");
				map.Add((int)SqlDbType.UniqueIdentifier, "Guid");
				map.Add((int)SqlDbType.TinyInt, "byte");
				map.Add((int)SqlDbType.Date, "DateTime");
				map.Add((int)SqlDbType.SmallMoney, "Double");
				map.Add((int)SqlDbType.Money, "Double");
			}
        }
		
		public List<Field> GetColumns(string table)
		{
			List<Field> list = new List<Field>();
			DbConnection conn = null;
			try
			{

				DbCommand cmd;
				if (database == EDatabase.MySQL)
				{
					conn = new MySqlConnection(connString);
					conn.Open();
					cmd = new MySqlCommand("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table.ToUpper() + "' order by column_name", (MySqlConnection)conn);
				}
				else
				{
					conn = new SqlConnection(connString);
					conn.Open();
					cmd = new SqlCommand("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table.ToUpper() + "' order by column_name", (SqlConnection)conn);
				}
				DbDataReader reader = cmd.ExecuteReader();
				if(reader != null)
				{
					while(reader.Read())
					{
						string colname = reader.GetString(3);
						string dbtype = reader.GetString(7);
						if(list.FindIndex(delegate(Field find) { return find.dbname == colname; }) < 0)
						{
							Field f = new Field();
							f.classname = f.dbname = colname;
							object o = null;
							if (database == EDatabase.MySQL)
							{
								//Exceptions that are not found in the enum, but the database seems to allow
								if (dbtype == "int")
									dbtype = "Int32";
								else if (dbtype == "tinyint")
									dbtype = "byte";

								o = Enum.Parse(typeof(MySqlDbType), dbtype, true);
							}
							else
								o = Enum.Parse(typeof(SqlDbType), dbtype, true);
							f.datatype = (int)o;
							list.Add(f);
						}
					}
					reader.Close();
				}

				return list;
			}
			catch(Exception e)
			{
				//Logger.Error(0, deviceId, "Exception in GetUpdateVersion", e);
			}
			finally
			{
				if(conn != null)
					conn.Close();
			}

			return null;
		}

		public string DBtoCode(string column)
		{
			int diff = 'a' - 'A';
			StringBuilder sb = new StringBuilder(column.Length);
			char[] ca = column.ToLower().ToCharArray();

			bool newWord = true;

			for(int i = 0; i < ca.Length; i++)
			{
				char c = ca[i];
				
				if(newWord)
					c = (char)(Convert.ToInt32(ca[i]) - diff);

				if(c == '_' || c == ' ')
				{
					newWord = true;
				}
				else
				{
					sb.Append(c);
					newWord = false;
				}
			}

			return sb.ToString();
		}

		public string GetColNamesClass(List<Field> fields)
		{
			StringBuilder sb = new StringBuilder();
			sb.AppendLine("\tpublic class Col");
			sb.AppendLine(("\t{"));

			for(int i = 0; i < fields.Count; i++)
			{
				string col = fields[i].dbname;
				string field = DBtoCode(col);
				sb.Append("\t\tpublic static string ");
				sb.Append(field);
				sb.Append(" = ");
				sb.Append("\"" + col + "\";");
				sb.AppendLine();
			}

			sb.AppendLine(("\t}"));
			return sb.ToString();
		}

		public string GetColNamesClass(string table)
		{
			return GetColNamesClass(GetColumns(table));
		}

		public string GetClassFields(string table)
		{
			List<Field> fields = GetColumns(table);
			StringBuilder sb = new StringBuilder();

			for(int i = 0; i < fields.Count; i++)
			{
				string col = fields[i].classname;
				string field = DBtoCode(col);
				string type = map[fields[i].datatype];
				sb.Append("\t public ");
				sb.Append(type);
				sb.Append(" ");
				sb.Append(field);
				sb.Append(";");
				
				sb.AppendLine();
			}

			sb.AppendLine();
			sb.AppendLine();
			sb.AppendLine("	public static string TableName = \"" + table + "\";");
			sb.Append(GetColNamesClass(table));


			return sb.ToString();

		}

		public string GetAssignments(string table, bool direction)
		{
			List<Field> fields = GetColumns(table);
			StringBuilder sb = new StringBuilder();

			//advertiser.Address = tbAddress.Text;
            // First character lower case and singular
            string obName = char.ToLower(table[0]) + Regex.Replace(table.Substring(1), "s$", "");

			for(int i = 0; i < fields.Count; i++)
			{
				string col = fields[i].classname;
				string field = DBtoCode(col);
				string type = map[fields[i].datatype];
				sb.Append("\t");

				if(direction)
				{
					sb.Append(obName + "." + field);
					sb.Append(" = tb");
					sb.Append(field);
					sb.Append(".Text;");
				}
				else
				{
					sb.Append("tb");
					sb.Append(field);
					sb.Append(".Text");
					sb.Append(" = ");
					sb.Append(obName + "." + field);
					sb.Append(";");
				}
				sb.AppendLine();
			}


			return sb.ToString();
		}

		public string GetClass(string table)
		{
            string singular = Regex.Replace(table, "s$", "");

			StringBuilder sb = new StringBuilder();
            sb.AppendLine("public class " + singular);
			sb.AppendLine(("{"));
			
//			sb.AppendLine();
			sb.Append(GetClassFields(table));
			sb.AppendLine(("}"));
			sb.AppendLine();
			sb.AppendLine();
			sb.AppendLine(("/*"));
			sb.AppendLine(GetAssignments(table, true));
			sb.AppendLine();
			sb.AppendLine(GetAssignments(table, false));


			sb.AppendLine(("*/"));

			return sb.ToString();
		}

		public string GetForm(string table)
		{
// 	<tr>
// 		<td><asp:Label ID="Label1" runat="server" Text="Label"></asp:Label>	</td>
// 		<td><asp:TextBox ID="TextBox1" runat="server"></asp:TextBox></td>
// 	</tr>
	
			List<Field> fields = GetColumns(table);
			StringBuilder sb = new StringBuilder();

			for(int i = 0; i < fields.Count; i++)
			{
				string col = fields[i].classname;
				string field = DBtoCode(col);
				string type = map[fields[i].datatype];
				sb.AppendLine("<tr>");
				sb.AppendFormat("\t<td>{0}</td>\n", field);
				sb.AppendFormat("\t<td>\n\t\t<asp:TextBox ID=\"tb{0}\" runat=\"server\"></asp:TextBox>\n\t</td>\n", field);
				sb.AppendLine("</tr>");
			}



			return sb.ToString();
		}
	}
}
