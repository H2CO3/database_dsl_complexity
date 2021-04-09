using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text.Json;
using System.Collections.Generic;


namespace DatabaseDemo {

public static class CaseConverter {
    private static Regex CamelRegex = new Regex(@"[A-Z][^A-Z]*");

    public static string ToSnakeCase(this string input) {
        return String.Join("_", CamelRegex.Matches(input).Select(m => m.Value)).ToLower();
    }

    public static JsonElement ToSnakeCaseKeyJsonElement(
        this JsonElement element
    ) {
        var dict = element.EnumerateObject().ToDictionary(
            kv => kv.Name.ToSnakeCase(),
            kv => kv.Value
        );
        return JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(dict));
    }
}

}
