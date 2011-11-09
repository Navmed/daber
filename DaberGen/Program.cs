using System;
using System.Collections.Generic;
using System.Text;


namespace DaberGen
{
	class Program
	{
		static void Main(string[] args)
		{
			if(args.Length != 3)
			{
				Console.WriteLine("DaberGen generates classes based on a database table");
				Console.WriteLine("Usage: <database (mysql or sqlserver)> \"<connection string>\" <table>");
			}
			else
			{
				DB db = new DB(args[0], args[1]);	
				string s = db.GetClass(args[2]);
				Console.WriteLine(s);
			}
		}
	}


}
