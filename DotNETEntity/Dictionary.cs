using System.Collections.Generic;

namespace DatabaseDemo {

public static class DictionaryExt {
    public static TValue GetOrDefault<TKey, TValue>(
        this Dictionary<TKey, TValue> dict,
        TKey key,
        TValue defaultValue
    )
        where TKey: notnull
    {
        TValue value;

        if (dict.TryGetValue(key, out value)) {
            return value;
        } else {
            return defaultValue;
        }
    }
}

}
