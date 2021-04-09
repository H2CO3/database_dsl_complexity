import Foundation
import SwiftSyntax
import CodableCSV


func main(infile: String, outfile: String) throws {
  let inurl = URL(fileURLWithPath: infile)
  let outurl = URL(fileURLWithPath: outfile)

  let ast = try SyntaxParser.parse(inurl)
  let visitor = MetricsVisitor()
  visitor.walk(ast)

  let metrics = visitor.tokens.queries.sorted().map {
    Metrics(query: $0, visitor: visitor)
  }

  let encoder = CSVEncoder()
  encoder.headers = Metrics.CodingKeys.allCases.map { $0.rawValue }
  try encoder.encode(metrics, into: outurl)
}

try main(infile: CommandLine.arguments[1], outfile: CommandLine.arguments[2])
