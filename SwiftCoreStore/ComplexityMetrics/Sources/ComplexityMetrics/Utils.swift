import Foundation


extension String {
  func strip() -> String {
    return self.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  /// This is simple and super inefficient, but that's fine.
  func toSnakeCase() -> String {
    let regex = try! NSRegularExpression(pattern: "([^A-Z])([A-Z]*)([A-Z])", options: [])
    let splitted = regex.stringByReplacingMatches(
      in: self,
      options: [],
      range: NSRange(location: 0, length: count),
      withTemplate: "$1_$2_$3"
    )
    return splitted.replacingOccurrences(of: "__", with: "_").lowercased()
  }
}

extension Substring {
  func strip() -> String {
    return self.trimmingCharacters(in: .whitespacesAndNewlines)
  }
}
