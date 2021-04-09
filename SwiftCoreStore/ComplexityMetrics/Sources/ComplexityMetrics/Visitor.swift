import SwiftSyntax


typealias CK = SyntaxVisitorContinueKind

class MetricsVisitor: SyntaxVisitor {
  // MARK: Instance Variables

  /// Name of the query being visited, `nil` for non-query functions.
  private var currentQuery: String? = nil

  /// Operators and Operands by Query Name
  private(set) var operators = TokenCounter()
  private(set) var operands = TokenCounter()

  /// All the tokens, for token entropy.
  private(set) var tokens = TokenCounter()

  /// Unweighted AST node count
  private(set) var nodeCount: [String: UInt] = [:]
  /// Weighted AST node count
  private(set) var weightedNodeCount: [String: UInt] = [:]

  // MARK: Overridden Methods

  override func visit(_ node: FunctionDeclSyntax) -> CK {
    let prefix = "query"

    if node.identifier.text.hasPrefix(prefix) {
      currentQuery = String(node.identifier.text.dropFirst(prefix.count)).toSnakeCase()

      // Manually walk body so that only expression operators are picked up,
      // and declarations aren't (unfairly) counted towards the complexity.
      if let body = node.body {
        walk(body)
      }
    }

    return .skipChildren
  }

  // MARK: - Declarations

  override func visitPost(_ node: FunctionDeclSyntax) {
    // Do not count tokens and nodes outside of a query
    // function towards the complexity of the latest query.
    currentQuery = nil
  }

  /// A variable declaration or name binding.
  override func visit(_ node: VariableDeclSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  /// Equivalent to a variable declaration.
  override func visit(_ node: ClosureCaptureItemSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  /// Equivalent to a variable declaration.
  override func visit(_ node: ClosureParamSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: TypeInitializerClauseSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ParameterClauseSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ReturnClauseSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  // MARK: - Types

  override func visit(_ node: TupleTypeElementSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: GenericArgumentSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: TypeAnnotationSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SimpleTypeIdentifierSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ArrayTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: DictionaryTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: OptionalTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ImplicitlyUnwrappedOptionalTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SomeTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: TupleTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: FunctionTypeSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  // MARK: - Patterns

  // Variable names are visited here as well
  override func visit(_ node: IdentifierPatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: EnumCasePatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: IsTypePatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: AsTypePatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: OptionalPatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: TuplePatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: WildcardPatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ExpressionPatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ValueBindingPatternSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  // MARK: - Statements

  override func visit(_ node: ContinueStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: BreakStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ReturnStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ForInStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: WhileStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: RepeatWhileStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: GuardStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  // We have to count optional-binding-as-a-condition separately,
  // because these bindings do NOT get represented by a regular
  // variable binding AST node.
  override func visit(_ node: OptionalBindingConditionSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ExpressionStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: DeclarationStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: IfStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ElseBlockSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: ElseIfContinuationSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SwitchStmtSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SwitchCaseSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SwitchDefaultLabelSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: CaseItemSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SwitchCaseLabelSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  // MARK: - Expressions

  override func visit(_ node: ArrayExprSyntax) -> CK {
    add(nodeWithWeight: 3) // array literal ~ SQL `VALUES` expression
    return .visitChildren
  }

  override func visit(_ node: AsExprSyntax) -> CK {
    add(nodeWithWeight: 2) // equivalent of `CAST`
    return .visitChildren
  }

  override func visit(_ node: AssignmentExprSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: BinaryOperatorExprSyntax) -> CK {
    let weights: [String: UInt] = [
      "+" : 2,
      "+=": 2, // same as `+`
      "-" : 2,
      "-=": 2,
      "*" : 2,
      "*=": 2,
      "/" : 2,
      "/=": 2,
      "==": 2,
      "!=": 3,
      "<" : 3,
      ">" : 2,
      "<=": 3,
      ">=": 3,
      "%" : 5,
      "&&": 2,
      "||": 2,
      "??": 3, // equivalent of SQL's `COALESCE`
      "~=": 3, // defined by CoreStore: set containment, approx. SQL's `IN`
    ]
    let op = node.description.strip()

    add(nodeWithWeight: weights[op] ?? 1)
    return .visitChildren
  }

  override func visit(_ node: ClosureExprSyntax) -> CK {
    add(nodeWithWeight: 1) // no direct SQL equivalent
    return .visitChildren
  }

  override func visit(_ node: DictionaryExprSyntax) -> CK {
    add(nodeWithWeight: 3) // closest to SQL's `VALUES` clause (name-value pairs)
    return .visitChildren
  }

  override func visit(_ node: ForcedValueExprSyntax) -> CK {
    add(nodeWithWeight: 1) // no equivalent in SQL
    return .visitChildren
  }

  override func visit(_ node: FunctionCallExprSyntax) -> CK {
    // The non-qualified name of the callee is the last component in its path.
    // We are also not interested in the exact type of e.g. `From<...>` clauses,
    // so the generic argument list is stripped, if any.
    let genericCallee = node.calledExpression.description.split(separator: ".").last!
    let callee = genericCallee.split(separator: "<").first!.strip()

    // Some functions have special meaning: they are the fundamental building
    // blocks of queries.
    let weights: [String: UInt] = [
      "select": 2,
      "Select": 2,
      "queryAttributes": 2, // ~SELECT
      "fetchOne": 2, // ~SELECT
      "fetchAll": 2, // ~SELECT
      "map": 2, // ~SELECT
      "flatMap": 2, // ~SELECT
      "compactMap": 2, // ~SELECT
      "From": 2,
      "where": 2,
      "Where": 2,
      "filter": 2, // ~WHERE
      "groupBy": 2,
      "GroupBy": 2,
      "orderBy": 2,
      "sort": 2, // ~ORDER BY
      "sorted": 2, // ~ORDER BY
      "count": 2,
      "max": 2,
      "maximum": 2,
      "min": 3,
      "sum": 2,
    ]

    add(nodeWithWeight: weights[callee] ?? 1)
    return .visitChildren
  }

  override func visit(_ node: IdentifierExprSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: IntegerLiteralExprSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: IsExprSyntax) -> CK {
    add(nodeWithWeight: 2) // type casting related operator
    return .visitChildren
  }

  override func visit(_ node: KeyPathExprSyntax) -> CK {
    add(nodeWithWeight: 1) // a key path is essentially just a type-safe identifier
    return .visitChildren
  }

  override func visit(_ node: MemberAccessExprSyntax) -> CK {
    add(nodeWithWeight: 1) // also, just an identifier
    return .visitChildren
  }

  override func visit(_ node: NilLiteralExprSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: OptionalChainingExprSyntax) -> CK {
    add(nodeWithWeight: 1) // no direct SQL equivalent, most functions pass through NULL
    return .visitChildren
  }

  override func visit(_ node: PrefixOperatorExprSyntax) -> CK {
    let op = node.description.strip()
    let weight: UInt

    if op.hasPrefix("!") {
      weight = 2 // logical NOT
    } else if op.hasPrefix("..<") {
      weight = 3 // subslicing, approximately `LIMIT` or `FETCH FIRST`
    } else {
      weight = 1
    }

    add(nodeWithWeight: weight)
    return .visitChildren
  }

  override func visit(_ node: SpecializeExprSyntax) -> CK {
    add(nodeWithWeight: 1) // no direct SQL equivalent
    return .visitChildren
  }

  override func visit(_ node: StringLiteralExprSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  override func visit(_ node: SubscriptExprSyntax) -> CK {
    add(nodeWithWeight: 1) // no direct SQL equivalent
    return .visitChildren
  }

  override func visit(_ node: TernaryExprSyntax) -> CK {
    add(nodeWithWeight: 2)
    return .visitChildren
  }

  override func visit(_ node: TryExprSyntax) -> CK {
    add(nodeWithWeight: 1) // no real equivalent in SQL
    return .visitChildren
  }

  override func visit(_ node: TupleExprSyntax) -> CK {
    add(nodeWithWeight: 1)
    return .visitChildren
  }

  // MARK: - Tokens

  override func visit(_ token: TokenSyntax) -> CK {
    let token = Token(token)

    tokens.add(token, to: currentQuery)

    switch token.kind {
      // Operators
      case .letKeyword:                  add(operator: token)
      case .varKeyword:                  add(operator: token)
      case .deferKeyword:                add(operator: token)
      case .ifKeyword:                   add(operator: token)
      case .guardKeyword:                add(operator: token)
      case .doKeyword:                   add(operator: token)
      case .repeatKeyword:               add(operator: token)
      case .elseKeyword:                 add(operator: token)
      case .forKeyword:                  add(operator: token)
      case .inKeyword:                   add(operator: token)
      case .whileKeyword:                add(operator: token)
      case .returnKeyword:               add(operator: token)
      case .breakKeyword:                add(operator: token)
      case .continueKeyword:             add(operator: token)
      case .fallthroughKeyword:          add(operator: token)
      case .switchKeyword:               add(operator: token)
      case .caseKeyword:                 add(operator: token)
      case .defaultKeyword:              add(operator: token)
      case .whereKeyword:                add(operator: token)
      case .catchKeyword:                add(operator: token)
      case .asKeyword:                   add(operator: token)
      case .isKeyword:                   add(operator: token)
      case .throwKeyword:                add(operator: token)
      case .tryKeyword:                  add(operator: token)
      case .poundAssertKeyword:          add(operator: token)
      case .poundSourceLocationKeyword:  add(operator: token)
      case .poundSelectorKeyword:        add(operator: token)
      case .poundKeyPathKeyword:         add(operator: token)
      case .arrow:                       add(operator: token)
      case .atSign:                      add(operator: token)
      case .colon:                       add(operator: token)
      case .semicolon:                   add(operator: token)
      case .comma:                       add(operator: token)
      case .period:                      add(operator: token)
      case .equal:                       add(operator: token)
      case .prefixPeriod:                add(operator: token)
      case .leftParen:                   add(operator: token)
      case .rightParen:                  break // do not add twice
      case .leftBrace:                   add(operator: token)
      case .rightBrace:                  break // do not add twice
      case .leftSquareBracket:           add(operator: token)
      case .rightSquareBracket:          break // do not add twice
      case .leftAngle:                   add(operator: token)
      case .rightAngle:                  break // do not add twice
      case .prefixAmpersand:             add(operator: token)
      case .postfixQuestionMark:         add(operator: token)
      case .infixQuestionMark:           add(operator: token)
      case .exclamationMark:             add(operator: token)
      case .backslash:                   add(operator: token)
      case .unspacedBinaryOperator(_):   add(operator: token)
      case .spacedBinaryOperator(_):     add(operator: token)
      case .prefixOperator(_):           add(operator: token)
      case .postfixOperator(_):          add(operator: token)
      case .yield:                       add(operator: token)

      // Operands
      case .falseKeyword:                add(operand: token)
      case .nilKeyword:                  add(operand: token)
      case .selfKeyword:                 add(operand: token)
      case .trueKeyword:                 add(operand: token)
      case .__file__Keyword:             add(operand: token)
      case .__line__Keyword:             add(operand: token)
      case .__column__Keyword:           add(operand: token)
      case .__function__Keyword:         add(operand: token)
      case .poundFileKeyword:            add(operand: token)
      case .poundLineKeyword:            add(operand: token)
      case .poundColumnKeyword:          add(operand: token)
      case .poundFunctionKeyword:        add(operand: token)
      case .stringQuote:                 break // string literal delimiter
      case .multilineStringQuote:        break // idem
      case .stringSegment(_):            add(operand: token)
      case .identifier(_):               add(operand: token)
      case .dollarIdentifier(_):         add(operand: token)
      case .integerLiteral(_):           add(operand: token)
      case .floatingLiteral(_):          add(operand: token)

      default:
        if let query = currentQuery {
          fatalError("unhandled token in query `\(query)`: \(token)")
        }
    }

    return .visitChildren
  }
}

extension MetricsVisitor {
  private func add(operator token: Token) {
    operators.add(token, to: currentQuery)
  }

  private func add(operand token: Token) {
    operands.add(token, to: currentQuery)
  }

  private func add(nodeWithWeight weight: UInt) {
    guard let key = currentQuery else { return }

    nodeCount[key, default: 0] += 1
    weightedNodeCount[key, default: 0] += weight
  }
}
