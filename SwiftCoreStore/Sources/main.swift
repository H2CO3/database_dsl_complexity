import Foundation
import CoreStore

func main() throws {
    let outputPath = "db.sqlite3"
    let testPath = "../ExampleData/query_results_before_migration.json"

    // We are not doing this "properly", this code is prone
    // to a TOCTOU race condition, but it does not matter,
    // because we are only ever using the DB file from here.
    let alreadyLoaded = FileManager.default.fileExists(atPath: outputPath)
    let dataStack = try makeDataStack(at: outputPath)

    if alreadyLoaded {
        print("Warning: Database at \(outputPath) already exists; not reloading data\n")
    } else {
        let sqlDB = try SQLiteDB(path: "../ExampleData/example_data_before_migration.sqlite3")
        let dataLoader = DataLoader(sqlDB: sqlDB, dataStack: dataStack)

        try dataLoader.load()
    }

    let queries = Queries(dataStack: dataStack)
    let tests = try TestCases(path: testPath)
    let results = try tests.runAll(functions: queries.queries)
    var failed = false

    for (name, result) in results {
        switch result {
        case .success(let elapsed):
            print(String(format: "%@ Succeeded in %.4f s", name, elapsed))

        case .failure(let error):
            failed = true
            print(error)
        }

        print("----------------")
    }

    if failed {
        throw TestError.someTestsFailed
    }
}

try! main()
