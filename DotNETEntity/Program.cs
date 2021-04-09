using System;
using System.IO;
using System.Collections.Generic;
using System.Text.Json;
using System.Linq;
using System.Reflection;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;


namespace DatabaseDemo {

class Program {
    private static DemoDbContext PopulateDatabaseIfNeeded(string inputPath, string outputPath) {
        if (File.Exists(outputPath)) {
            Console.WriteLine("Warning: Database at {0} already exists; not reloading data", outputPath);
            return new DemoDbContext(outputPath);
        }

        using var loader = new DataLoader(inputPath);
        var db = new DemoDbContext(outputPath);

        loader.PopulateDatabase(db);

        return db;
    }

    private delegate IQueryable<Object> EvaluateQuery(JsonElement[] args);

    private static IEnumerable<QueryResult> ExecuteQueries(Queries queries, Tests tests) {
        var prefix = "Query";
        var testCases = (
            from fn
            in queries.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
            where fn.Name.StartsWith(prefix)
            select new {
                Fn = fn.CreateDelegate(typeof(EvaluateQuery), queries),
                Name = fn.Name.Substring(prefix.Length).ToSnakeCase(),
            }
        ).ToList();

        Debug.Assert(
            testCases.Count == tests.TestCases.Count,
            $"There are {testCases.Count} queries but {tests.TestCases.Count} tests"
        );

        var results = new List<QueryResult>();

        foreach (var tc in testCases) {
            var expected = tests.TestCases[tc.Name];
            var startTime = DateTime.Now;
            var lazy = tc.Fn.DynamicInvoke(new Object[]{ expected.Params }) as IQueryable<Object>;
            var actual = lazy!.ToArray();
            var endTime = DateTime.Now;
            var result = new QueryResult {
                Name = tc.Name,
                ElapsedTime = endTime - startTime,
                ActualResult = actual,
                ExpectedResult = expected.Results,
                SQL = lazy!.ToQueryString(),
                ViolationIndexes = expected.ViolationIndexes(actual),
            };

            results.Add(result);
        }

        return results;
    }

    static int Main(string[] args) {
        using var db = PopulateDatabaseIfNeeded(
            "../ExampleData/example_data_before_migration.sqlite3",
            "db.sqlite3"
        );
        var queries = new Queries(db);
        var testJson = File.ReadAllText("../ExampleData/query_results_before_migration.json");
        var tests = new Tests(testJson);
        var results = ExecuteQueries(queries, tests);

        foreach (var result in results) {
            Console.WriteLine(result);
        }

        // Compute code complexity metrics for each query
        var metrics = new Metrics("Queries.cs");
        metrics.WriteToCSV("metrics.csv");

        // Return error exit status if any of the queries failed
        if (results.Where(r => r.Failed).Any()) {
            return 1;
        } else {
            return 0;
        }
    }
}

class QueryResult {
    public String Name = String.Empty;
    public TimeSpan ElapsedTime;
    public Object[] ActualResult = null!;
    public JsonElement[] ExpectedResult = {};
    public String SQL = String.Empty;
    public IEnumerable<int> ViolationIndexes = null!;

    public bool Failed => ViolationIndexes.Any();

    public override string ToString() {
        if (Failed) {
            var idxString = String.Join(
                ", ",
                from i in ViolationIndexes select i.ToString()
            );

            var jsonOptions = new JsonSerializerOptions {
                WriteIndented = true,
            };
            var actualJson = JsonSerializer.Serialize(ActualResult, jsonOptions);
            var expectedJson = JsonSerializer.Serialize(ExpectedResult, jsonOptions);

            return @$"{Name} FAILED
Mismatch indexes: {idxString}

Actual results: {actualJson}

Expected results: {expectedJson}

SQL Statement:
{SQL}
----------------";
        } else {
            return $"{Name} Succeeded in {ElapsedTime}\n----------------";
        }
    }
}

}
