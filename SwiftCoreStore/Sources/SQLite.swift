import Foundation
import SQLite3

enum SQLiteError: Error {
    case openFailed(path: String)
    case queryFailed(reason: String)
    case bindFailed(index: Int, reason: String)
}

/// Class for raw, untyped, but simple access to SQLite
final class SQLiteDB {
    public let dateFormatter: DateFormatter

    private let db: OpaquePointer
    private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    init(path: String) throws {
        var tmpDb: OpaquePointer!
        guard sqlite3_open(path, &tmpDb) == SQLITE_OK else {
            throw SQLiteError.openFailed(path: path)
        }

        db = tmpDb

        dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
    }

    deinit {
        sqlite3_close(db)
    }

    /// Returns the latest SQLite error message.
    /// It should be called *only* when we *know*
    /// that an error has occurred previously.
    private var error: String {
        String(cString: sqlite3_errmsg(db))
    }

    /// Internal helper for binding a parameter to a prepared statement.
    private func bind(_ param: Any?, at index: Int, to stmt: OpaquePointer) throws {
        let i = Int32(index)
        var bindFailed: SQLiteError {
            SQLiteError.bindFailed(index: index, reason: error)
        }

        guard let p = param else {
            guard sqlite3_bind_null(stmt, i) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if p is NSNull {
            guard sqlite3_bind_null(stmt, i) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pBool = p as? Bool {
            guard sqlite3_bind_int(stmt, i, pBool ? 1 : 0) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pInt = p as? Int {
            guard sqlite3_bind_int64(stmt, i, Int64(pInt)) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pInt = p as? Int32 {
            guard sqlite3_bind_int(stmt, i, pInt) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pInt = p as? Int64 {
            guard sqlite3_bind_int64(stmt, i, pInt) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pInt = p as? UInt {
            guard sqlite3_bind_int64(stmt, i, Int64(pInt)) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pInt = p as? UInt32 {
            guard sqlite3_bind_int(stmt, i, Int32(pInt)) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pInt = p as? UInt64 {
            guard sqlite3_bind_int64(stmt, i, Int64(pInt)) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pFloat = p as? Float {
            guard sqlite3_bind_double(stmt, i, Double(pFloat)) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pFloat = p as? Double {
            guard sqlite3_bind_double(stmt, i, pFloat) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pStr = p as? String {
            let len = Int32(pStr.utf8.count)
            guard sqlite3_bind_text(stmt, i, pStr, len, SQLITE_TRANSIENT) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pBlob = p as? Data {
            let len = pBlob.count
            let bytes = malloc(len).bindMemory(to: UInt8.self, capacity: len)
            pBlob.copyBytes(to: bytes, count: len)

            guard sqlite3_bind_blob(stmt, i, bytes, Int32(len), free) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        if let pDate = p as? Date {
            let dateStr = dateFormatter.string(from: pDate)
            let len = Int32(dateStr.utf8.count)
            guard sqlite3_bind_text(stmt, i, dateStr, len, SQLITE_TRANSIENT) == SQLITE_OK else {
                throw bindFailed
            }
            return
        }

        throw SQLiteError.bindFailed(index: index, reason: "unsupported type")
    }

    /// Internal helper for retrieving the next row
    /// from a currently running prepared statement.
    private func next(from stmt: OpaquePointer, with names: [String]) throws -> [String: Any?]? {
        switch sqlite3_step(stmt) {
        case SQLITE_ROW:
            let len = names.count
            var row = Dictionary<String, Any?>(minimumCapacity: len)

            for index in 0..<len {
                let name = names[index]
                row[name] = try column(from: stmt, at: Int32(index))
            }

            return row

        case SQLITE_OK, SQLITE_DONE:
            return nil

        default:
            throw SQLiteError.queryFailed(reason: error)
        }
    }

    /// Internal helper for retrieving a specified column
    /// after stepping the prepared statement to the next row.
    private func column(from stmt: OpaquePointer, at index: Int32) throws -> Any? {
        switch sqlite3_column_type(stmt, index) {
        case SQLITE_NULL:
            return nil
        case SQLITE_INTEGER:
            return sqlite3_column_int64(stmt, index)
        case SQLITE_FLOAT:
            return sqlite3_column_double(stmt, index)
        case SQLITE_TEXT:
            return String(cString: sqlite3_column_text(stmt, index))
        case SQLITE_BLOB:
            let bytes = sqlite3_column_blob(stmt, index)
            let count = sqlite3_column_bytes(stmt, index)
            return Data(bytes: bytes!, count: Int(count))
        default:
            throw SQLiteError.queryFailed(reason: "unknown result type")
        }
    }

    /// The public interface: prepare and execute `sql` with `params`,
    /// and return the results as an array of rows, mapping column
    /// names to their respective values.
    func query(_ sql: String, params: [Any?] = []) throws -> [[String: Any?]] {
        var stmt: OpaquePointer!

        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw SQLiteError.queryFailed(reason: error)
        }

        defer {
            sqlite3_finalize(stmt)
        }

        for (index, param) in params.enumerated() {
            try bind(param, at: index, to: stmt)
        }

        var rows: [[String: Any?]] = []
        var names: [String] = []
        let numCols = sqlite3_column_count(stmt)

        for i in 0..<numCols {
            let cName = sqlite3_column_name(stmt, i)
            names.append(String(cString: cName!))
        }

        while let row = try next(from: stmt, with: names) {
            rows.append(row)
        }

        return rows
    }
}
