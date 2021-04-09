import Foundation
import SwiftSyntax


struct Metrics {
  let name: String
  let tokenCount: UInt
  let tokenEntropy: Double
  let nodeCount: UInt
  let weightedNodeCount: UInt
  let halsteadVocabulary: Double
  let halsteadLength: Double
  let halsteadEstimatedLength: Double
  let halsteadVolume: Double
  let halsteadDifficulty: Double
  let halsteadEffort: Double
}

extension Metrics: Codable {
  enum CodingKeys: String, CodingKey, CaseIterable {
    case name = "query"
    case tokenCount = "token_count"
    case tokenEntropy = "token_entropy"
    case nodeCount = "node_count"
    case weightedNodeCount = "weighted_node_count"
    case halsteadVocabulary = "halstead_vocabulary"
    case halsteadLength = "halstead_length"
    case halsteadEstimatedLength = "halstead_estimated_length"
    case halsteadVolume = "halstead_volume"
    case halsteadDifficulty = "halstead_difficulty"
    case halsteadEffort = "halstead_effort"
  }
}

extension Metrics {
  init(query: String, visitor: MetricsVisitor) {
    name = query
    tokenCount = visitor.tokens.totalTokenCount(for: query)
    tokenEntropy = visitor.tokens.tokenProbabilities(for: query).values.sorted().reduce(0) {
      $0 - $1 * log($1)
    }
    nodeCount = visitor.nodeCount[query] ?? 0
    weightedNodeCount = visitor.weightedNodeCount[query] ?? 0

    let eta1 = Double(visitor.operators.uniqueTokenCount(for: query))
    let eta2 = Double(visitor.operands.uniqueTokenCount(for: query))
    let n1 = Double(visitor.operators.totalTokenCount(for: query))
    let n2 = Double(visitor.operands.totalTokenCount(for: query))

    let eta = eta1 + eta2
    let n = n1 + n2
    let nHat = eta1 * log2(eta1) + eta2 * log2(eta2)
    let v = n * log2(eta)
    let d = eta1 / 2.0 * n2 / eta2
    let e = d * v

    halsteadVocabulary = eta
    halsteadLength = n
    halsteadEstimatedLength = nHat
    halsteadVolume = v
    halsteadDifficulty = d
    halsteadEffort = e
  }
}
