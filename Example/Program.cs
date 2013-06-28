// This is an example progam that uses the NorthWind database to show the capabilities of daber

using System;
using System.Collections.Generic;
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

			// More complex examples
			// Join: Get all products of a certain category name
			List<Product> list = db.GetListQuery<Product>(@"SELECT ProductID FROM PRODUCTS AS p
										JOIN Categories AS c ON c.CategoryID=p.CategoryID
										WHERE CategoryName='Beverages'");

			// Another way of getting the above count
			int count = db.Get<int>(Product.TableName, "COUNT(0)", 
									Product.Col.SupplierID, 1);

			// Get count of Products from supplier 1
			count = db.GetQuery<int>("SELECT COUNT(0) FROM Products WHERE SupplierId=@supplierId", 1);

			// Get Categories with low stock, which have few items on order
			List<int> lowStock = db.GetListQuery<int>("SELECT CategoryId FROM Products GROUP BY CategoryId HAVING SUM(UnitsInStock) - SUM(UnitsOnOrder) < @v0", 200);

			// Get the difference between the Units in Stock and Unit on Order for Category 1
			int diff = db.Get<int>(Product.TableName, "SUM(UnitsInStock) - SUM(UnitsOnOrder)",
			                       Product.Col.CategoryID, 1);

			// Set the Reorder Level to the sum of the Unit in Stock + Units on Order, for items have stock below 5 and are not discontinued
			db.Update(Product.TableName, null, 1,
			          Product.Col.ReorderLevel + "=", Product.Col.UnitsInStock + "+" + Product.Col.UnitsOnOrder,
			          Product.Col.UnitsInStock+"<", 5,
			          Product.Col.Discontinued, 0);

		}
	}
}
