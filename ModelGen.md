# Model Code Generation #

Use "dabergen.exe" to generate the model for each table.

## Usage ##
`dabergen <mysql|sqlserver> "<connection string>" <table> [/a] [/hf] [/wf]`

### Parameters ###
  1. The first parameter is either "mysql" or "sqlserver" to indicate the database type
  1. The Connection String
  1. The Table name
  1. Optional: /a Use to generate field assignments
  1. Optional: /hf Use this to automatically generate code for an HTML form
  1. Optional: /wf Use this to automatically generate code for a Web form

### Example ###
Here is the (simplified) Products table in the NorthWind database

```
CREATE TABLE Products (
	ProductID int NOT NULL IDENTITY PRIMARY KEY, 
	ProductName nvarchar(40),
	SupplierID int, 
	CategoryID int, 
	QuantityPerUnit nvarchar(20), 
	UnitPrice money, 
	UnitsInStock smallint, 
	UnitsOnOrder smallint, 
	ReorderLevel smallint, 
	Discontinued bit
)
```

To generate a C# model class for this table, call this from the command-line:

```
DaberGen.exe SQLServer "Server=localhost;Database=Northwind;Trusted_Connection=True;" "Products"
```

This generates the following code in the console for the **Products** table on the **NorthWind** database for SQLServer.

```
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


        public static string TableName = "Products1";
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
```


### Writing the model to a file ###
Use redirection to write it to a file.
```
DaberGen.exe SQLServer "Server=localhost;Database=Northwind;Trusted_Connection=True;" "Products" > Product.cs
```