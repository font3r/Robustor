using System.Reflection;
using DbUp;

namespace Robustor;

public static class Migrations
{
    public static void InitOutbox(string connectionString)
    {
        var upgradeEngine = DeployChanges.To
            .SqlDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(Assembly.GetExecutingAssembly())
            .Build();

        var result = upgradeEngine.PerformUpgrade();
        if (result.Successful) return;
        
        Console.WriteLine(result.Error.Message);
        Console.WriteLine(result.ErrorScript.Contents);
        // Do not allow application to start
    }
}