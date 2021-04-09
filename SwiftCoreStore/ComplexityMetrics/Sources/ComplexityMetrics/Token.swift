import SwiftSyntax


/// This struct only exists so that tokens aren't hashed
/// and compared based on their unique ID. Instead, they
/// are hashed and compared based on their token kind
/// and textual contents.
struct Token {
  private let token: TokenSyntax
}

extension Token {
  init(_ token: TokenSyntax) {
    self.token = token
  }

  var kind: TokenKind {
    return token.tokenKind
  }
}

extension Token: Hashable {
  public func hash(into hasher: inout Hasher) {
    self.token.withoutTrivia().text.hash(into: &hasher)
  }
}

extension Token: Equatable {
  static func ==(lhs: Token, rhs: Token) -> Bool {
    return lhs.token.tokenKind == rhs.token.tokenKind
  }
}

extension Token: CustomStringConvertible {
  var description: String {
    return token.text
  }
}

extension Token: CustomDebugStringConvertible {
  var debugDescription: String {
    return String(describing: kind)
  }
}


struct TokenCounter {
  /// Maps query name to Token -> Count mapping
  private var counter: [String: [Token: UInt]] = [:]
}

extension TokenCounter {
  mutating func add(_ token: Token, to query: String?) {
    guard let query = query else { return }
    counter[query, default: [:]][token, default: 0] += 1
  }

  var queries: [String] {
    return Array(counter.keys)
  }

  func tokenCounts(for query: String) -> [Token: UInt] {
    return counter[query] ?? [:]
  }

  func totalTokenCount(for query: String) -> UInt {
    return tokenCounts(for: query).values.reduce(0, +)
  }

  func uniqueTokenCount(for query: String) -> UInt {
    return UInt(tokenCounts(for: query).count)
  }

  func tokenProbabilities(for query: String) -> [Token: Double] {
    let counts = tokenCounts(for: query)
    let total = Double(totalTokenCount(for: query))

    return Dictionary(
      uniqueKeysWithValues: counts.map { ($0.0, Double($0.1) / total) }
    )
  }
}
