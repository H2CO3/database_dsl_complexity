using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Linq;
using System.Reflection;


namespace DatabaseDemo {

public class TestCase {
    [JsonPropertyName("params")]
    public JsonElement[] Params { get; set; }
    [JsonPropertyName("results")]
    public JsonElement[] Results { get; set; }

    [JsonConstructor]
    public TestCase(JsonElement[] Params, JsonElement[] Results) {
        // Round-trip through JSON string because this is
        // the only portable way of obtaining a JsonElement
        this.Params = Params;
        this.Results = JsonSerializer.Deserialize<JsonElement[]>(
            JsonSerializer.Serialize(Results)!
        )!;
    }

    // Deep equality for loosely-typed values, so that
    // exact types like `Int32` vs `Int64` don't matter
    private static bool JsonEquals(JsonElement x, JsonElement y) {
        if (x.ValueKind != y.ValueKind) {
            return false;
        }

        switch (x.ValueKind) {
        case JsonValueKind.Undefined:
        case JsonValueKind.Null:
        case JsonValueKind.True:
        case JsonValueKind.False:
            return true;

        case JsonValueKind.Number:
            // the safest representation
            return Math.Abs(x.GetDecimal() - y.GetDecimal()) <= new Decimal(1e-9);

        case JsonValueKind.String:
            return x.GetString() == y.GetString(); // resolves escape sequences

        case JsonValueKind.Array:
            if (x.GetArrayLength() != y.GetArrayLength()) {
                return false;
            }

            foreach (var xy in x.EnumerateArray().Zip(y.EnumerateArray())) {
                if (!JsonEquals(xy.First, xy.Second)) {
                    return false;
                }
            }

            return true;

        case JsonValueKind.Object:
            if (x.EnumerateObject().Count() != y.EnumerateObject().Count()) {
                return false;
            }

            // NB: this is done like this because order should not matter
            foreach (var p in x.EnumerateObject()) {
                JsonElement q;

                if (!y.TryGetProperty(p.Name, out q)) {
                    return false;
                }

                if (!JsonEquals(p.Value, q)) {
                    return false;
                }
            }

            return true;

        default:
            return false;
        }
    }

    public IEnumerable<int> ViolationIndexes(IEnumerable<Object> rows) {
        // Round-trip the result set through JSON, because this is
        // the only public way of obtaining `JsonElement`s for
        // equality comparison (this does not work if we obtain
        // the property values directly, via reflection, because
        // apparently equality comparison in C# is *hard...*)
        //
        // Also convert results to snake_case because that
        // is how the gold standard results are specified.
        var list = JsonSerializer.Deserialize<List<JsonElement>>(
            JsonSerializer.Serialize(rows)
        )!.Select(
            row => row.ToSnakeCaseKeyJsonElement()
        );

        if (list!.Count() < Results.Count()) {
            return Enumerable.Range(list.Count(), Results.Count() - list.Count());
        } else if (Results.Count() < list.Count()) {
            return Enumerable.Range(Results.Count(), list.Count() - Results.Count());
        }

        return list!
            .Zip(Results)
            .Select((actExp, i) => (i, !JsonEquals(actExp.First, actExp.Second)))
            .Where(idxEq => idxEq.Item2)
            .Select(idxEq => idxEq.Item1);
    }
}

public class Tests {
    public Dictionary<String, TestCase> TestCases { get; init; }

    public Tests(string json) {
        TestCases = JsonSerializer.Deserialize<Dictionary<String, TestCase>>(json)!;
    }
}

}
