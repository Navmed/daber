﻿/*
The contents of this file are subject to the Mozilla Public License Version 1.1 (the "License"); 
you may not use this file except in compliance with the License. You may obtain a copy of the 
License at http://www.mozilla.org/MPL/ 

Software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTY OF 
ANY KIND, either express or implied. See the License for the specific language governing rights and 
limitations under the License. 

The Original Code is Daber

The Initial Developer of the Original Code is Naveed Ahmed are Copyright (C) 2010. All Rights Reserved. 
*/




using System;
using System.Data;
using System.Configuration;
using System.Web;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Reflection;
using System.Text.RegularExpressions;

using System.Data.Common;
using MySql.Data.MySqlClient;




public delegate void DLogError(string s, Exception e);
public class Daber
{
	Dictionary<string, List<FieldInfo>> FieldInfoCache = new Dictionary<string, List<FieldInfo>>();     // Cache of all the fields of a class
	Dictionary<string, Dictionary<string, int>> DBFieldIndexMaps = new Dictionary<string, Dictionary<string, int>>();	// Each dictionary contains an index of the column in the db for the respective field
	Dictionary<string, Dictionary<string, string>> DBFieldMaps = new Dictionary<string, Dictionary<string, string>>();	// Mapping between class field (key) and db field (value)

	protected List<string> ignoredClasses = new List<string>();
	protected List<string> ignoredFields = new List<string>();
	public List<string> IgnoredFields { get { return (this.ignoredFields); } set { this.ignoredFields = value; } }

	/// <summary>
	/// Use this to allow insertion of ids when identity is NOT set. Normally this field is ignored while inserting assuming that id is an identity column
	/// </summary>
	public bool InsertId = false;
	string IDCOL = "Id";

	protected string connString;
	public string ConnString { get { return (this.connString); } set { this.connString = value; } }

	protected DLogError logError;
	public DLogError LogError { get { return (this.logError); } set { this.logError = value; } }

	public enum EDatabase
	{
		NotSpecified = 0,
		SQLServer = 1,
		MySQL = 2
	}

	protected EDatabase database = EDatabase.NotSpecified;

	public Daber(string connectionString, EDatabase database)
	{
		this.connString = connectionString;
		this.database = database;
	}

	DbCommand connect()
	{
		DbConnection conn = null;
		switch(database)
		{		
			case EDatabase.SQLServer: 
				conn = new SqlConnection(connString);
				break;
			
			case EDatabase.MySQL: 
				conn = new MySqlConnection(connString);
				break;

			default: return null;
		}

		conn.Open();
		DbCommand cmd = conn.CreateCommand();
		return cmd;
	}

	void close(DbCommand cmd)
	{
		if (cmd != null && cmd.Connection != null && cmd.Connection.State != ConnectionState.Closed)
			cmd.Connection.Close();
	}

	public int ExecuteNonQuery(string cmdText, params object[] list)
	{
		DbCommand cmd = null;

		int rows = 0;
		try
		{
			cmd = connect();

			for (int i = 0; i < list.Length; i++)
			{
				string col = "v" + i;
				object o = list[i];
				addParameter(cmd, col, o);
			}

			cmd.CommandText = cmdText;
			rows = cmd.ExecuteNonQuery();
		}
		catch (Exception ex)
		{
			if (logError != null)
				logError("Exception in DB.ExecuteNonQuery", ex);

			rows = -1;
		}
		finally
		{
			close(cmd);
		}

		return rows;
	}

	public T Get<T>(string table, string getColumn, params object[] list)
	{
		List<T> l = GetList<T>(table, getColumn, null, null, list);
		if (l == null || l.Count == 0)
			return default(T);
		return l[0];
	}

	public T GetWithCondition<T>(string table, string getColumn, string extraCondition, params object[] list)
	{
		List<T> l = GetList<T>(table, getColumn, null, extraCondition, list);
		if (l == null || l.Count == 0)
			return default(T);
		return l[0];
	}





	/// <summary>
	/// Adds parameters to the cmd object. The parameter values are in list. 
	/// The order of the parameters in the query should match the order of parameter values
	/// The parameters can be repeated in the query, but the order of parameters in the query
	/// should still match the parameter values
	/// </summary>
	/// <param name="cmd"></param>
	/// <param name="list"></param>
	void addParameters(DbCommand cmd, params object[] list)
	{
		if (list == null || list.Length == 0)
			return;

		HashSet<string> parms = new HashSet<string>();

		Regex rgx = new Regex("@[0-9a-z_]+", RegexOptions.IgnoreCase);
		MatchCollection matches = rgx.Matches(cmd.CommandText);

		int parNum = 0;
		foreach (Match match in matches)
		{
			string parName = match.Value;
			if (!parms.Contains(parName))
			{
				parms.Add(parName);

				addParameter(cmd, parName, list[parNum]);
				parNum++;
			}
		}
	}

	/// <summary>
	/// Returns a list of objects of the class T. T should have the same fields specified in the select query in cmdText
	///
	/// The order of the parameters in the query should match the order of parameter values
	/// The parameters can be repeated in the query, but the order of parameters in the query
	/// should still match the parameter values
	/// </summary>
	/// <typeparam name="T">Class of the return type</typeparam>
	/// <param name="cmdText">Select Query</param>
	/// <param name="list">List of parameter values for the query</param>
	/// <returns></returns>
	public List<T> GetListQuery<T>(string cmdText, params object[] list)
	{
		//DbCommand cmd = new DbCommand();
		DbCommand cmd = connect();
		return getList<T>(cmd, cmdText, list);
	}

	/// <summary>
	/// Returns an object of the class T. T should have the same fields specified in the select query in cmdText
	///
	/// The order of the parameters in the query should match the order of parameter values
	/// The parameters can be repeated in the query, but the order of parameters in the query
	/// should still match the parameter values
	/// </summary>
	/// <typeparam name="T">Class of the return type</typeparam>
	/// <param name="cmdText">Select Query</param>
	/// <param name="list">List of parameter values for the query</param>
	/// <returns></returns>

	public T GetQuery<T>(string cmdText, params object[] list)
	{
		List<T> l = GetListQuery<T>(cmdText, list);
		if (l == null || l.Count == 0)
			return default(T);
		return l[0];
	}


	/// <summary>
	/// Get a list of rows from a table
	/// list has column name and value pairs. The default condition operation is '=', to use <, >, <=, =>, != OR 'LIKE' add the operator at the end of the column
	/// The default conjunction between column name value pairs is 'AND'. Suffix ' OR' to use OR instead.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <param name="table"></param>
	/// <param name="getColumn"></param>
	/// <param name="orderColumn"></param>
	/// <param name="extraCondition"></param>
	/// <param name="list"></param>
	/// <returns></returns>
	public List<T> GetList<T>(string table, string getColumn, string orderColumn, string extraCondition, params object[] list)
	{
		if (list.Length % 2 != 0)
		{
			throw new Exception("Number of columns does not match number of values in GetList");
		}

		string cmdText = "";
		//DbCommand cmd = new DbCommand();
		DbCommand cmd = connect();
		StringBuilder condition = null;

		condition = new StringBuilder(buildCondition(cmd, list));


		if (!string.IsNullOrEmpty(extraCondition))
		{
			if (condition.ToString() != "")
				condition.Append(" AND ");
			condition.Append(" " + extraCondition);
		}

		if (condition.Length > 0)
			cmdText = string.Format("SELECT {0} FROM {1} WHERE {2}", getColumn, table, condition);
		else
			cmdText = string.Format("SELECT {0} FROM {1}", getColumn, table);

		if (!string.IsNullOrEmpty(orderColumn))
			cmdText += string.Format(" ORDER BY {0}", orderColumn);

		return getList<T>(cmd, cmdText);
	}


	List<T> getList<T>(DbCommand cmd, string cmdText, params object[] list)
	{
		List<T> ret = new List<T>();
		

		try
		{
			//cmd = connect();

			cmd.CommandText = cmdText;
			addParameters(cmd, list);

			DbDataReader reader = cmd.ExecuteReader();
			if (reader != null)
			{
				Type obType = typeof(T);
				List<FieldInfo> fields = getFields(typeof(T));
				Dictionary<string, int> dbFieldIndexMap = null;
				if (IsClass(obType))
					dbFieldIndexMap = getDBFieldIndexMap(typeof(T), reader);

				while (reader.Read())
				{
					object o = null;
					if (IsClass(obType))
					{
						// 							if(!getColumn.EndsWith("*"))
						// 								throw new Exception("SELECT should use * for class selections");

						ConstructorInfo ci = obType.GetConstructor(new Type[] { });
						if (ci == null)
							throw new Exception(string.Format("{0} does not have a default constructor", obType.Name));

						o = ci.Invoke(null);

						for (int i = 0; i < reader.FieldCount; i++)
						{
							string dbField = reader.GetName(i);
							if (!dbFieldIndexMap.ContainsKey(dbField))
								continue;
							int fieldIndex = dbFieldIndexMap[dbField];
							FieldInfo fi = fields[fieldIndex];
							string fieldName = fi.Name;

							object dbValue = reader[i];

							if (dbValue.GetType() == typeof(DBNull))
							{
								// do nothing
							}
							else if (dbValue.GetType() == typeof(Int64) && fi.FieldType == typeof(Int32))
							{
								Int32 n = Convert.ToInt32(dbValue);
								fi.SetValue(o, n);
							}
							else if (dbValue.GetType() == typeof(string) && fi.FieldType == typeof(Guid))
							{
								string s = dbValue.ToString();
								fi.SetValue(o, new Guid(s));
							}
							else if (fi.FieldType.IsEnum)
							{
								if (dbValue.GetType() == typeof(decimal))
									fi.SetValue(o, Enum.ToObject(fi.FieldType, Decimal.ToInt32((decimal)dbValue)));
								else
									fi.SetValue(o, Enum.ToObject(fi.FieldType, dbValue));
							}
							else if (fi.FieldType == typeof(double) || fi.FieldType == typeof(double?))
							{
								double n = Convert.ToDouble(dbValue);
								fi.SetValue(o, n);
							}
							else if (fi.FieldType == typeof(int) && dbValue.GetType() == typeof(decimal))
								fi.SetValue(o, Decimal.ToInt32((decimal)dbValue));
							else if (fi.FieldType == typeof(long) && dbValue.GetType() == typeof(decimal))
								fi.SetValue(o, Decimal.ToInt64((decimal)dbValue));
							else if (fi.FieldType == typeof(bool))
								fi.SetValue(o, Convert.ToBoolean(dbValue));
							else if (fi.FieldType == typeof(DateTime))
								fi.SetValue(o, Convert.ToDateTime(dbValue));
							else if (!IsArray(fi))
								fi.SetValue(o, dbValue);
							else if (fi.FieldType.IsEnum)
							{
								if (Enum.GetUnderlyingType(fi.FieldType) == typeof(Byte))
									fi.SetValue(o, Convert.ToByte(dbValue));
								else if (Enum.GetUnderlyingType(fi.FieldType) == typeof(short))
									fi.SetValue(o, Convert.ToInt16(dbValue));
								else if (Enum.GetUnderlyingType(fi.FieldType) == typeof(int))
									fi.SetValue(o, Convert.ToInt32(dbValue));
							}
							else
								throw new Exception(string.Format("Could not handle field '{0}' of type '{0}' in query '{1}'", fieldName, fi.FieldType, cmdText));
						}

						ret.Add((T)o);
					}
					else
					{
						if (reader.IsDBNull(0))
							ret.Add(default(T));
						else if (typeof(T) == typeof(Int32) || typeof(T) == typeof(Int32?))
							ret.Add((T)(object)Convert.ToInt32(reader.GetInt32(0)));
						else if (typeof(T) == typeof(string))
							ret.Add((T)(object)reader.GetString(0));
						else if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateTime?))
							ret.Add((T)(object)reader.GetDateTime(0));
						else if (typeof(T) == typeof(ulong) || typeof(T) == typeof(long))
							ret.Add((T)(object)reader.GetInt64(0));
						else if (typeof(T) == typeof(Guid))
							ret.Add((T)(object)reader.GetGuid(0));
						else if (typeof(T) == typeof(short))
							ret.Add((T)(object)reader.GetInt16(0));
						else if (typeof(T) == typeof(Decimal))
							ret.Add((T)(reader.GetValue(0)));
						else if (typeof(T) == typeof(double))
						{
							object d = Convert.ToDouble(reader.GetValue(0));
							ret.Add((T)d);
						}

						else if (typeof(T) == typeof(byte))
							ret.Add((T)(reader.GetValue(0)));
						else if (typeof(T) == typeof(bool) || typeof(T) == typeof(bool?))
						{
							ret.Add((T)(reader.GetValue(0)));

						}
						else
							throw new Exception("Return type not supported: " + typeof(T));
					}
				}
				reader.Close();
			}
		}
		catch (Exception e)
		{
			if (logError != null)
			{
				StringBuilder condition = new StringBuilder();
				getLoggableConditionString(condition, list);
				logError(string.Format("Exception in GetList {0}", condition.ToString()), e);
			}
		}
		finally
		{
			close(cmd);
		}
		return ret;
	}

	/// <summary>
	/// Converts the condition string to a loggable kind - replaces the parameter place holders (@v0, @v1 ...) with actual values
	/// </summary>
	/// <param name="sb"></param>
	/// <param name="list"></param>
	protected static void getLoggableConditionString(StringBuilder sb, object[] list)
	{
		for (int i = 0; i < list.Length; i += 2)
		{
			object obValue = null;
			int index = (int)(i / 2);
			if (i + 1 < list.Length)
				obValue = list[i + 1];
			string valString = "@v" + index;
			sb.Replace(valString, obValue.ToString());
		}
	}

	/// <summary>
	/// Converts db field name format to class field format - removes underscores and capitalizes the words
	/// </summary>
	/// <param name="dbFieldName"></param>
	/// <returns></returns>
	public static string DBToCodeFieldName(string column)
	{
		int diff = 'a' - 'A';
		StringBuilder sb = new StringBuilder(column.Length);
		char[] ca = column.ToLower().ToCharArray();

		bool newWord = true;

		for (int i = 0; i < ca.Length; i++)
		{
			char c = ca[i];

			if (newWord)
				c = (char)(Convert.ToInt32(ca[i]) - diff);

			if (c == '_')
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




	/// <summary>
	/// Get the fields from a given Type. Cached. 
	/// </summary>
	/// <param name="t">the Type</param>
	/// <returns></returns>
	List<FieldInfo> getFields(Type t)
	{
		List<FieldInfo> fields = null;
		if (FieldInfoCache.ContainsKey(t.Name))
			fields = FieldInfoCache[t.Name];
		else
		{
			FieldInfo[] _fields = t.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
			fields = new List<FieldInfo>(_fields);
			fields.Sort(CompareFieldInfo);
			FieldInfoCache.Add(t.Name, fields);
		}

		return fields;
	}


	/// <summary>
	/// Add a custom mapping of db Column to class field 
	/// </summary>
	/// <param name="list">alternate db column followed by class field name</param>
	public void AddFieldIndexMap(Type t, params string[] list)
	{
		if (list.Length % 2 != 0)
		{
			throw new Exception("Number of columns does not match number of fields in AddFieldIndexMap");
		}

		for (int i = 0; i < list.Length; i += 2)
		{
			string col = list[i];
			string classFieldName = list[i + 1];
		}


		Dictionary<string, int> map = new Dictionary<string, int>(list.Length / 2);
		List<FieldInfo> fields = getFields(t);

		for (int i = 0; i < list.Length; i += 2)
		{
			string dbFieldName = list[i];
			string classFieldName = list[i + 1];

			for (int j = 0; j < fields.Count; j++)
			{
				if (fields[j].Name.Equals(classFieldName, StringComparison.CurrentCultureIgnoreCase))
				{
					map.Add(dbFieldName, j);
					break;
				}
			}
		}

		DBFieldIndexMaps.Add(t.Name, map);
	}



	Dictionary<string, int> getDBFieldIndexMap(Type t, DbDataReader reader)
	{
		Dictionary<string, int> map = null;
		if (DBFieldIndexMaps.ContainsKey(t.Name))
			map = DBFieldIndexMaps[t.Name];
		else
		{
			map = new Dictionary<string, int>(reader.FieldCount);
			List<FieldInfo> fields = getFields(t);

			for (int i = 0; i < reader.FieldCount; i++)
			{
				string dbFieldName = reader.GetName(i);
				string codeFieldName = DBToCodeFieldName(dbFieldName);

				for (int j = 0; j < fields.Count; j++)
				{
					if (fields[j].Name.Equals(codeFieldName, StringComparison.CurrentCultureIgnoreCase) || fields[j].Name.Equals(dbFieldName, StringComparison.CurrentCultureIgnoreCase))
					{
						map.Add(dbFieldName, j);
						break;
					}
				}
			}

			DBFieldIndexMaps.Add(t.Name, map);
		}

		return map;
	}


	/// <summary>
	/// Builds the condition part of the query
	/// list has column name and value pairs. The default condition operation is '=', to use '>' or '<' add the operator at the end of the column
	/// </summary>
	/// <param name="cmd"></param>
	/// <param name="list"></param>
	/// <returns></returns>
	protected string buildCondition(DbCommand cmd, params object[] list)
	{
		if (cmd == null || list == null || list.Length == 0)
			return "";

		string AND = " AND ";
		string OR = " OR ";
		string sConj = AND;
		char[] whiteSpace = " \t\r\n".ToCharArray();

		StringBuilder sbCondition = new StringBuilder();
		for (int i = 0; i < list.Length; i += 2)
		{
			sConj = AND;
			object oVal = list[i + 1];
			string col = list[i].ToString();

			if (col.EndsWith(OR.TrimEnd(whiteSpace), StringComparison.CurrentCultureIgnoreCase))
			{
				col = col.Remove(col.Length - OR.TrimEnd(whiteSpace).Length);
				sConj = OR;
			}

			if (oVal == null || (oVal is string && oVal.ToString() == ""))
			{
				if (col.LastIndexOf("!=") == col.Length - 2)
					sbCondition.Append(col.Substring(0, col.Length - 2) + " IS NOT NULL" + sConj);
				else
					sbCondition.Append(col + " IS NULL" + sConj);
			}
			else
			{
				string parName = "@v" + ((int)(i / 2)).ToString();

				string like = " LIKE";
				if (col.Length > like.Length && col.Substring(col.Length - like.Length).Equals(like, StringComparison.CurrentCultureIgnoreCase))
					sbCondition.Append(col + " " + parName + sConj);
				else if (col.LastIndexOf(">=") == col.Length - 2)
					sbCondition.Append(col.Substring(0, col.Length - 2) + " >= " + parName + sConj);
				else if (col.LastIndexOf("<=") == col.Length - 2)
					sbCondition.Append(col.Substring(0, col.Length - 2) + " <= " + parName + sConj);
				else if (col.LastIndexOf("!=") == col.Length - 2)
					sbCondition.Append(col.Substring(0, col.Length - 2) + " != " + parName + sConj);
				else if (col.LastIndexOf('>') == col.Length - 1)
					sbCondition.Append(col.Substring(0, col.Length - 1) + " > " + parName + sConj);
				else if (col.LastIndexOf('<') == col.Length - 1)
					sbCondition.Append(col.Substring(0, col.Length - 1) + " < " + parName + sConj);
				else
					sbCondition.Append(col + " = " + parName + sConj);

				addParameter(cmd, parName, oVal);
			}
		}

		sbCondition.Remove(sbCondition.Length - sConj.Length, sConj.Length);	// Remove the AND
		return sbCondition.ToString();
	}

	/// <summary>
	/// Builds the condition string for logging. Mainly used for exception logging
	/// </summary>
	/// <param name="list"></param>
	/// <returns></returns>
	protected string buildLogCondition(params object[] list)
	{
		if (list == null || list.Length == 0)
			return "";

		string sAND = " AND ";
		StringBuilder sbCondition = new StringBuilder();
		for (int i = 0; i < list.Length; i += 2)
		{
			object oVal = list[i + 1];
			string col = list[i].ToString();

			if (oVal == null || (oVal is string && oVal.ToString() == ""))
			{
				sbCondition.Append(col + " IS NULL" + sAND);
			}
			else
			{
				string parName = oVal.ToString();

				if (col.LastIndexOf('>') == col.Length - 1)
					sbCondition.Append(col.Substring(0, col.Length - 1) + " > " + parName + sAND);
				else if (col.LastIndexOf('<') == col.Length - 1)
					sbCondition.Append(col.Substring(0, col.Length - 1) + " < " + parName + sAND);
				else
					sbCondition.Append(col + " = " + parName + sAND);
			}
		}

		sbCondition.Remove(sbCondition.Length - sAND.Length, sAND.Length);	// Remove the AND
		return sbCondition.ToString();

	}



	/// <summary>
	/// Update a table
	/// list has column name and value pairs. 
	/// The default condition operation is '=', to use '>', '<', '>=' or '<=' add the operator at the end of the column
	/// To set the value of one column from another column or more like col1 = col2 + col3, suffix the first column with '='
	/// </summary>
	/// <param name="table"></param>
	/// <param name="numUpdateCols">The number of column value pairs that are columns to be updated</param>
	/// <param name="list">First include the column-value pairs to be updated and then the column-value pair conditions</param>
	/// <returns>Number of rows updated. -1 for error/exception</returns>
	public int Update(string table, string extraCondition, int numUpdateCols, params object[] list)
	{
		int ret = 0;

		if (list.Length % 2 != 0)
		{
			throw new Exception("Number of columns does not match number of values in Update");
		}

		if (numUpdateCols > list.Length)
			throw new Exception("numUpdateCols > list.Length");

		DbCommand cmd = null;

		try
		{
			cmd = connect();

			object[] conditions = new object[list.Length - numUpdateCols * 2];
			Array.Copy(list, numUpdateCols * 2, conditions, 0, list.Length - numUpdateCols * 2);
			StringBuilder condition = new StringBuilder(buildCondition(cmd, conditions));

			if (!string.IsNullOrEmpty(extraCondition))
			{
				if (condition.ToString() != "")
					condition.Append(" AND ");
				condition.Append(" " + extraCondition);
			}

			StringBuilder updates = new StringBuilder();
			for (int i = 0; i < numUpdateCols * 2; i += 2)
			{
				object oVal = list[i + 1];
				string col = list[i].ToString();

				if (col.EndsWith("="))
				{
					col = col.Substring(0, col.Length - 1);	// remove the '='
					updates.AppendFormat("{0} = {1},", col, oVal);
				}
				else
				{
					updates.AppendFormat("{0}=@updateValue{1},", col, i / 2);
					addParameter(cmd, "@updateValue" + i / 2, oVal);
				}
			}

			updates.Remove(updates.Length - 1, 1);	// Remove the comma

			cmd.CommandText = string.Format("UPDATE {0} SET {1} WHERE {2}", table, updates, condition);
			ret = cmd.ExecuteNonQuery();
		}
		catch (Exception e)
		{
			if (logError != null)
				logError("Exception in Update " + buildLogCondition(list), e);

			ret = -1;
		}
		finally
		{
			close(cmd);
		}

		return ret;
	}

	public int Delete(string table, params object[] list)
	{
		if (list.Length % 2 != 0)
		{
			throw new Exception("Number of columns does not match number of values in Delete");
		}

		int ret = 0;


		DbCommand cmd = null;

		try
		{
			cmd = connect();
			
			

			string condition = buildCondition(cmd, list);

			if (list == null || list.Length == 0 || string.IsNullOrEmpty(condition))
				cmd.CommandText = string.Format("DELETE FROM {0}", table);
			else
				cmd.CommandText = string.Format("DELETE FROM {0} WHERE {1}", table, condition);

			ret = cmd.ExecuteNonQuery();
		}
		catch (Exception e)
		{
			if (logError != null)
				logError("Exception in Delete from" + table + " WHERE " + buildLogCondition(list), e);

			ret = -1;
		}
		finally
		{
			close(cmd);
		}

		return ret;
	}

	protected Dictionary<string, string> getDBFieldMap(string table, Type t)
	{
		if (DBFieldMaps.ContainsKey(table))
			return DBFieldMaps[table];

		List<FieldInfo> fields = getFields(t);


		DbCommand cmd = null;

		try
		{
			cmd = connect();
			cmd.CommandText = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table.ToUpper() + "' order by column_name";

			DbDataReader reader = cmd.ExecuteReader();
			if (reader == null)
				return null;

			Dictionary<string, string> map = new Dictionary<string, string>(fields.Count);

			while (reader.Read())
			{
				string dbFieldName = reader.GetString(3);
				string codeFieldName = DBToCodeFieldName(dbFieldName);

				for (int j = 0; j < fields.Count; j++)
				{
					if (fields[j].Name.Equals(codeFieldName, StringComparison.CurrentCultureIgnoreCase))
					{
						map.Add(fields[j].Name, dbFieldName);
						break;
					}
				}
			}

			DBFieldMaps.Add(table, map);
			return map;
		}
		catch (Exception ex)
		{
			if (logError != null)
				logError("Exception in getDBFieldMap", ex);
		}
		finally
		{
			close(cmd);
		}

		return null;
	}


	public int Insert<T>(string table, T item)
	{
		if (item == null)
			return 0;

		List<T> list = new List<T>(1);
		list.Add(item);
		return Insert<T>(table, list);
	}

	public int Insert<T>(string table, T item, out long id)
	{
		id = 0;
		if (item == null)
			return 0;

		List<T> list = new List<T>(1);
		list.Add(item);
		return Insert<T>(table, list, out id);
	}


	public int Insert<T>(string table, List<T> list)
	{
		long id = 0;
		return Insert<T>(table, list, out id);
	}

	/// <summary>
	/// Insert list of multiple objects
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <param name="table"></param>
	/// <param name="list"></param>
	/// <returns></returns>
	public int Insert<T>(string table, List<T> list, out long id)
	{
		id = 0;
		if (list == null || list.Count == 0)
			return 0;

		List<FieldInfo> fields = getFields(typeof(T));
		Dictionary<string, string> dbFieldMap = getDBFieldMap(table, typeof(T));
		int rows = 0;



		DbCommand cmd = null;

		try
		{
			cmd = connect();

			for (int i = 0; i < list.Count; i++)
			{
				StringBuilder sbCols = new StringBuilder();
				StringBuilder sbVals = new StringBuilder();

				cmd.Parameters.Clear();

				object oVal = list[i];

				for (int j = 0; j < fields.Count; j++)
				{
					FieldInfo fi = fields[j];
					if (Ignore(fi))
						continue;

					if (!InsertId && fi.Name.Equals(IDCOL, StringComparison.CurrentCultureIgnoreCase))
						continue;

					string col = dbFieldMap[fi.Name];
					object fieldValue = fi.GetValue(oVal);
					addToInsertString(cmd, j, col, fieldValue, ref sbCols, ref sbVals);
				}

				sbCols.Remove(sbCols.Length - 2, 2);	// Remove the comma
				sbVals.Remove(sbVals.Length - 2, 2);	// Remove the comma

				if (database == EDatabase.SQLServer)
				{
					cmd.CommandText = string.Format("INSERT INTO {0} ({1}) VALUES ({2})  SET @newId = SCOPE_IDENTITY()", table, sbCols.ToString(), sbVals.ToString());
					
					SqlParameter newId = new SqlParameter("@newId", SqlDbType.BigInt);
					newId.Direction = ParameterDirection.Output;
					cmd.Parameters.Add(newId);

					rows += cmd.ExecuteNonQuery();
					if (newId.Value is DBNull)
						id = 0;
					else
						id = Convert.ToInt64(newId.Value);
				}
				else if (database == EDatabase.MySQL)
				{
					cmd.CommandText = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", table, sbCols.ToString(), sbVals.ToString());

					//todo: do the mysql version to get last inserted id. Use LAST_INSERT_ID()

					rows += cmd.ExecuteNonQuery();
					id = 0;
				}

			}
		}
		catch (Exception ex)
		{
			if (logError != null)
			{
				StringBuilder sb = new StringBuilder();
				sb.AppendLine("Table: " + table);
				foreach (T item in list)
				{
					sb.AppendLine(ObjectString(item));
				}

				logError("InsertIntoDB: \r\n" + sb.ToString(), ex);
			}
			rows = -1;
		}
		finally
		{
			close(cmd);
		}

		return rows;
	}

	public bool Ignore(FieldInfo t)
	{
		int b = ignoredClasses.BinarySearch(t.FieldType.Name);
		if (b >= 0)
			return true;

		b = ignoredFields.BinarySearch(t.Name);
		if (b >= 0)
			return true;

		return CheckAttribute(t, "RxStream.DBIgnore");
	}

	public bool CheckAttribute(FieldInfo t, string attribName)
	{
		Object[] ca = t.GetCustomAttributes(true);
		foreach (Object attr in ca)
		{
			if (attr.ToString() == attribName)
				return true;
		}
		return false;
	}

	void addToInsertString(DbCommand cmd, int index, string col, object value, ref StringBuilder sbCols, ref StringBuilder sbValues)
	{
		sbCols.Append(col + ", ");

		string parName;

		if (value == null)
			parName = "NULL";
		else if (value != null && value.GetType() == typeof(string) && value.ToString().ToLower().IndexOf("nextval") > 0)	// Using sequence
			parName = value.ToString();
		else
		{
			parName = "@v" + (index.ToString());
			addParameter(cmd, parName, value);
		}

		sbValues.Append(parName + ", ");
	}

	static void addParameter(DbCommand cmd, string parName, object oVal)
	{
		if (cmd is SqlCommand)
			addParameter((SqlCommand)cmd, parName, oVal);
		else if(cmd is MySqlCommand)
			addParameter((MySqlCommand)cmd, parName, oVal);
	}

	static void addParameter(SqlCommand cmd, string parName, object oVal)
	{
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

	static void addParameter(MySqlCommand cmd, string parName, object oVal)
	{
		if (oVal == null)
			cmd.Parameters.Add(parName, MySqlDbType.VarChar).Value = DBNull.Value;
		else if (oVal.GetType() == typeof(string))
			cmd.Parameters.Add(parName, MySqlDbType.VarChar).Value = oVal;
		else if (oVal.GetType() == typeof(int))
			cmd.Parameters.Add(parName, MySqlDbType.Int32).Value = oVal;
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



	/// <summary>
	/// Returns an intersection of the provided lists
	/// </summary>
	/// <param name="a">Sorted list</param>
	/// <param name="b">Sorted list</param>
	/// <returns></returns>
	public static List<int> Intersect(List<int> a, List<int> b)
	{
		List<int> result = new List<int>(a.Count + b.Count);

		if (a == null || a.Count == 0)
			return result;
		else if (b == null || b.Count == 0)
			return result;

		int i = 0, j = 0;

		while (i < a.Count && j < b.Count)
		{
			while (i < a.Count && j < b.Count && a[i] < b[j])
				i++;

			bool added = false;
			while (i < a.Count && j < b.Count && a[i] == b[j])
			{
				if (!added)					// to avoid duplicates
					result.Add(a[i]);
				added = true;
				i++;
				j++;
			}

			while (i < a.Count && j < b.Count && a[i] > b[j])
				j++;
		}

		return result;
	}

	public static int CompareFieldInfo(FieldInfo x, FieldInfo y)
	{
		if (x == null)
		{
			if (y == null)
				return 0;
			else
				return -1;
		}
		else
		{
			if (y == null)
				return 1;
			else
			{
				if (x.Name == y.Name)
					return 0;

				if (x.Name == "id")
					return -1;
				if (y.Name == "id")
					return 1;

				if (IsClass(x.FieldType) != IsClass(y.FieldType))
				{
					if (IsClass(x.FieldType)) // We want classes to be at the end
						return 1;
					else
						return -1;
				}

				return x.Name.CompareTo(y.Name);
			}
		}
	}

	public static bool IsClass(Type t)
	{
		return (t.IsClass && t.Name != "String" && !IsStringArray(t));
	}

	public static bool IsArray(FieldInfo fi)
	{
		return (fi.FieldType.IsArray || (fi.FieldType.FullName.IndexOf("System.Collections.Generic.List") >= 0) && !IsStringArray(fi));
	}

	public static bool IsStringArray(FieldInfo fi)
	{
		return (fi.FieldType.FullName.IndexOf("System.Collections.Generic.List`1[[System.String") >= 0);
	}

	public static bool IsStringArray(Type t)
	{
		return (t.FullName.IndexOf("System.Collections.Generic.List`1[[System.String") >= 0);
	}

	/// <summary>
	/// Goes through all the fields in the object and generates a string
	/// </summary>
	/// <param name="o"></param>
	/// <returns></returns>
	public string ObjectString(object o, int indent = 1)
	{
		StringBuilder sb = new StringBuilder();

		List<FieldInfo> fields = getFields(o.GetType());
		string tabs = new string('\t', indent);

		for (int j = 0; j < fields.Count; j++)
		{
			FieldInfo fi = fields[j];
			if (Ignore(fi))
				continue;

			object fieldValue = fi.GetValue(o);
			sb.AppendLine(tabs + fi.Name + ": " + (fieldValue ?? "*NULL*"));
		}

		return sb.ToString();
	}
}
