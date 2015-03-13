# Introduction #
A quick and simple Database first .NET ORM libary meant to be small and efficient with low overhead, without have to relearn complex new API's.

# Goals #
  1. Use the power of SQL without rebuilding a complete new API
  1. Reduce the plumbing needed when making database calls
  1. Complexity should not increase exponentially with the complexity of the query, but linearly

# Simple usage #

```
// Initiate a new connection            
string connectionString = "Server=localhost;Database=testdb1;Trusted_Connection=True;";
IConnector connector = new SQLConnector(connectionString);
DB db = new DB(connector);

// Add a user
WebUser user = new WebUser { Username = "tom", 
            Firstname = "Tom", 
            Lastname="Smith", 
            City="Dallas", 
            State="OH" };
db.Insert(WebUser.TableName, user);

// Get the user whose username is 'tom'
WebUser u = db.Get<WebUser>(WebUser.TableName, "*", 
                            WebUser.Col.Username, "tom");
            
// Get all the users from OH
List<WebUser> users = db.GetList<WebUser>(WebUser.TableName, "*", null, null, 
                                          WebUser.Col.State, "OH"); 

// Set the state to "TX" for all users from Dallas
db.Update(WebUser.TableName, null, 1, 
          WebUser.Col.State, "TX", 
          WebUser.Col.City, "Dallas");

// Delete the user with username "tom"
db.Delete(WebUser.TableName, 
          WebUser.Col.Username, "tom");
```

[More Detailed Example](http://code.google.com/p/daber/source/browse/Example/Program.cs)