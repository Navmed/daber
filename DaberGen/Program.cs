using System;
using System.Collections.Generic;
using System.Text;


namespace DaberGen
{
	class Program
	{
		static void Main(string[] args)
		{
			if(args.Length < 3)
			{
				Console.WriteLine("DaberGen generates classes based on a database table");
				Console.WriteLine("Usage: dabergen <mysql|sqlserver> \"<connection string>\" <table> [/a] [/f]");
				Console.WriteLine("Parameters:");
				Console.WriteLine("1. Database type: mysql or sqlserve");
				Console.WriteLine("2. Connection String to the database");
				Console.WriteLine("3. Table Name");
				Console.WriteLine("4. Optional: /a: generate field assignments");
				Console.WriteLine("5. Optional: /wf: generate Web Forms");
				Console.WriteLine("5. Optional: /hf: generate HTML Forms");
			}
			else
			{
				DB db = new DB(args[0], args[1]);
				string table = args[2];
				StringBuilder sb = new StringBuilder( db.GetClass(table) );

				if(args.Length > 3)
				{
					for (int i = 3; i < args.Length; i++)
					{
						if (args[i] == "/wf")
							sb.AppendLine( "\r\n" + db.GetWebForm(table) );
						else if (args[i] == "/hf")
							sb.AppendLine("\r\n" + db.GetHTMLForm(table));
						else if(args[i] == "/a")
							sb.AppendLine("\r\n" + db.GetAssignments(table));
					}
				}
					
				Console.WriteLine(sb.ToString());
			}
		}
	}


}
