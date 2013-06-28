using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Daber;

namespace DaberExample
{
	public class Product
	{
		public int CategoryID;
		public bool Discontinued;
		public int ProductID;
		public string ProductName;
		public string QuantityPerUnit;
		public short ReorderLevel;
		public int SupplierID;
		public Double UnitPrice;
		public short UnitsInStock;
		public short UnitsOnOrder;


		public static string TableName = "Products";
		public class Col
		{
			public static string CategoryID = "CategoryID";
			public static string Discontinued = "Discontinued";
			public static string ProductID = "ProductID";
			public static string ProductName = "ProductName";
			public static string QuantityPerUnit = "QuantityPerUnit";
			public static string ReorderLevel = "ReorderLevel";
			public static string SupplierID = "SupplierID";
			public static string UnitPrice = "UnitPrice";
			public static string UnitsInStock = "UnitsInStock";
			public static string UnitsOnOrder = "UnitsOnOrder";
		}
	}
	
	
	class Program
	{
		static void Main(string[] args)
		{
			string connectionString = "Server=localhost;Database=Northwind;Trusted_Connection=True;";
			IConnector connector = new SQLConnector(connectionString);
			DB db = new DB(connector);

			// Get the product whose name is 'Chai'
			Product u = db.Get<Product>(Product.TableName, "*",
										Product.Col.ProductName, "Chai");

			// Get all the non-discontinued products with less than 5 units in stock
			List<Product> products = db.GetList<Product>(Product.TableName, "*", null, null,
			                                             Product.Col.Discontinued, false,
			                                             Product.Col.UnitsInStock+"<", 5);

			// Set all products UnitsOnOrder to 10 for products less than 5 in stock
			db.Update(Product.TableName, null, 1,
					  Product.Col.UnitsOnOrder, 10,
					  Product.Col.UnitsInStock + "<", 5);

			// Add a product
			Product p = new Product {CategoryID = 4, ProductName = "Apple Sauce", UnitPrice = 5, SupplierID = 1};
			db.Insert(Product.TableName, p);

			// Delete the user with username "tom"
			db.Delete(Product.TableName,
					  Product.Col.ProductName, "Apple Sauce");
		}
	}
}
