import Foundation

enum TestError: Error {
    case mismatchedTestCount(actual: Int, expected: Int)
    case malformedTestCase(name: String)
    case missingTestCase(name: String)
    case malformedFile(path: String)
    case testFailed(
        name: String,
        actual: [[String: Any]],
        expected: [[String: Any]],
        mismatchIndexes: [Int]
    )
    case someTestsFailed
}

extension TestError: CustomStringConvertible {
    var description: String {
        switch self {
        case .mismatchedTestCount(let actual, let expected):
            return "Found \(actual) tests but expected \(expected)"

        case .malformedTestCase(let name):
            return "Malformed test case: \(name)"

        case .missingTestCase(let name):
            return "Missing test case: \(name)"

        case .malformedFile(let path):
            return "Malformed file at \(path)"

        case .testFailed(let name, let actual, let expected, let indexes):
            return """
            \(name) FAILED

            Mismatch indexes: \(indexes)

            Actual results: \(try! prettyJSON(actual))

            Expected results: \(try! prettyJSON(expected))
            """

        case .someTestsFailed:
            return "Some tests failed"
        }
    }
}

final class TestCase {
    let name: String
    let params: [Any?]
    let expected: [[String: Any]]

    init(name: String, raw: [String: Any]) throws {
        guard let params = raw["params"] as? [Any?],
              let expected = raw["results"] as? [[String: Any]]
        else {
            throw TestError.malformedTestCase(name: name)
        }

        self.name = name
        self.params = params
        self.expected = expected
    }

    func evaluate(_ fn: ([Any?]) throws -> [[String: Any]]) throws -> Result<TimeInterval, TestError> {
        print("--------> Executing query \(name)")
        let start = Date()
        let actual = try fn(params)
        let end = Date()
        print("<-------- Executed query \(name)")
        let elapsed = end.timeIntervalSince(start)
        let minIdx = min(actual.count, expected.count)
        let maxIdx = max(actual.count, expected.count)

        guard minIdx == maxIdx else {
            return .failure(
                TestError.testFailed(
                    name: name,
                    actual: actual,
                    expected: expected,
                    mismatchIndexes: Array(minIdx..<maxIdx)
                )
            )
        }

        let mismatchIndexes = Array(
            zip(actual, expected).enumerated().filter {
                !jsonApproximatelyEquals(actual: $0.element.0, expected: $0.element.1)
            }.map {
                $0.offset
            }
        )

        guard mismatchIndexes.isEmpty else {
            return .failure(
                TestError.testFailed(
                    name: name,
                    actual: actual,
                    expected: expected,
                    mismatchIndexes: mismatchIndexes
                )
            )
        }

        return .success(elapsed)
    }
}

final class TestCases {
    let cases: [String: TestCase]

    init(path: String) throws {
        let url = URL(fileURLWithPath: path)
        let content = try Data(contentsOf: url)
        let json = try JSONSerialization.jsonObject(with: content)

        guard let raw = json as? [String: [String: Any]] else {
            throw TestError.malformedFile(path: path)
        }

        cases = Dictionary(
            uniqueKeysWithValues: try raw.map { (name, value) in
                (name, try TestCase(name: name, raw: value))
            }
        )
    }

    func runAll(functions: [String: ([Any?]) throws -> [[String: Any]]]) throws -> [String: Result<TimeInterval, TestError>] {
        guard functions.count == cases.count else {
            throw TestError.mismatchedTestCount(
                actual: functions.count,
                expected: cases.count
            )
        }

        return Dictionary(
            uniqueKeysWithValues: try functions.map { (name, fn) in
                guard let testCase = cases[name] else {
                    throw TestError.missingTestCase(name: name)
                }

                return (name, try testCase.evaluate(fn))
            }
        )
    }
}

func jsonApproximatelyEquals(actual: Any, expected: Any) -> Bool {
    if actual is NSNull && expected is NSNull {
        return true
    }

    if let x = actual as? Bool, let y = expected as? Bool {
        return x == y
    }

    if let x = actual as? Int64, let y = expected as? Int64 {
        return x == y
    }

    // Allow a tiny difference between floats
    if let x = actual as? Double, let y = expected as? Double {
        return abs(x - y) < 1e-7
    }

    if let x = actual as? String, let y = expected as? String {
        return x == y
    }

    if let x = actual as? [Any], let y = expected as? [Any] {
        guard x.count == y.count else {
            return false
        }

        return zip(x, y).allSatisfy {
            (i, j) in jsonApproximatelyEquals(actual: i, expected: j)
        }
    }

    if let x = actual as? [String: Any], let y = expected as? [String: Any] {
        guard x.count == y.count else {
            return false
        }

        // NB: implementation is such that the order of items does not matter
        for (k, i) in x {
            guard let j = y[k] else {
                return false
            }

            guard jsonApproximatelyEquals(actual: i, expected: j) else {
                return false
            }
        }

        return true
    }

    // any other combination of types indicates a mismatch
    return false
}

func prettyJSON(_ value: Any) throws -> String {
    let json = try JSONSerialization.data(
        withJSONObject: value,
        options: [
            .prettyPrinted,
            .fragmentsAllowed,
            .sortedKeys,
            .withoutEscapingSlashes,
        ]
    )

    return String(data: json, encoding: .utf8)!
}
