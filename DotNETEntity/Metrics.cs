using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using CsvHelper;
using CsvHelper.Configuration.Attributes;


namespace DatabaseDemo {

/// Collection of metrics for an individual query.
struct Metric {
    [Name("query")]
    public string Name { get; set; }

    [Name("token_count")]
    public int TokenCount { get; set; }

    [Name("token_entropy")]
    public double TokenEntropy { get; set; }

    [Name("node_count")]
    public int NodeCount { get; set; }

    [Name("weighted_node_count")]
    public int WeightedNodeCount { get; set; }

    [Name("halstead_vocabulary")]
    public int HalsteadVocabulary { get; set; }


    [Name("halstead_length")]
    public int HalsteadLength { get; set; }


    [Name("halstead_estimated_length")]
    public double HalsteadEstimatedLength { get; set; }


    [Name("halstead_volume")]
    public double HalsteadVolume { get; set; }


    [Name("halstead_difficulty")]
    public double HalsteadDifficulty { get; set; }


    [Name("halstead_effort")]
    public double HalsteadEffort { get; set; }

}

class Metrics {
    public Dictionary<String, Metric> MetricsByQuery
        = new Dictionary<String, Metric>();

    public Metrics(string sourceFile) {
        // Use Roslyn to parse the `Queries.cs` file
        // into a proper Abstract Syntax Tree, then
        // feed it to our visitor that filters out
        // non-query methods, and returns the relevant
        // parts of the structure of each query method.
        string source = File.ReadAllText(sourceFile);
        SyntaxTree ast = CSharpSyntaxTree.ParseText(source);

        // The visitor collects the tokens of the body
        // of each query, because this helps us computing
        // token-based metrics (token count and entropy)
        // later.
        // In addition, it also computes some node-based
        // metrics on its own, such as raw AS node count,
        // weighted AST node count, and the four basis
        // metrics of Halstead (eta1, eta2, n1, n2).
        MetricsVisitor visitor = new MetricsVisitor();
        visitor.Visit(ast.GetRoot());

        // Go through each query and actually compute metrics.
        foreach (var query in visitor.TokensForQuery) {
            var tokens = query.Value;

            var tokenCount = tokens.Count();
            var tokenEntropy = TokenEntropy(tokens);
            var nodeCountAndWeight = visitor.WeightsForQuery[query.Key];
            var ops = visitor.OpsForQuery[query.Key];
            var halstead = ComputeHalsteadMetrics(ops);

            this.MetricsByQuery[query.Key] = new Metric {
                Name = query.Key,
                TokenCount = tokenCount,
                TokenEntropy = tokenEntropy,
                NodeCount = nodeCountAndWeight.NodeCount,
                WeightedNodeCount = nodeCountAndWeight.NodeWeight,
                HalsteadVocabulary = halstead.Vocabulary,
                HalsteadLength = halstead.Length,
                HalsteadEstimatedLength = halstead.EstimatedLength,
                HalsteadVolume = halstead.Volume,
                HalsteadDifficulty = halstead.Difficulty,
                HalsteadEffort = halstead.Effort,
            };
        }
    }

    public void WriteToCSV(string filePath) {
        using (var streamWriter = new StreamWriter(filePath)) {
            using (var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture)) {
                csvWriter.WriteRecords(this.MetricsByQuery.Values);
            }
        }
    }

    private static double TokenEntropy(SyntaxToken[] tokens) {
        var probabilities = tokens
            .GroupBy(token => (token.ValueText, token.RawKind))
            .ToDictionary(
                group => group.Key,
                group => (double)group.Count() / (double)tokens.Count()
            );

        return -probabilities.Values.Select(p => p * Math.Log(p)).Sum();
    }

    private static HalsteadMetrics ComputeHalsteadMetrics(OperatorsAndOperands ops) {
        var operators = ops.Operators
            .GroupBy(op => op)
            .ToDictionary(group => group.Key, group => group.Count());
        var operands = ops.Operands
            .GroupBy(op => op)
            .ToDictionary(group => group.Key, group => group.Count());

        // The basis values
        int eta1 = operators.Count();
        int eta2 = operands.Count();
        int n1 = ops.Operators.Count();
        int n2 = ops.Operands.Count();

        // The computed values
        int eta = eta1 + eta2;
        int n = n1 + n2;
        double nHat = (double) eta1 * Math.Log2((double) eta1) + (double) eta2 * Math.Log2((double) eta2);
        double v = (double) n * Math.Log2((double) eta);
        double d = (double) eta1 / 2.0 * (double) n2 / (double) eta2;
        double e = d * v;

        return new HalsteadMetrics {
            Vocabulary = eta,
            Length = n,
            EstimatedLength = nHat,
            Volume = v,
            Difficulty = d,
            Effort = e,
        };
    }
}

class MetricsVisitor : CSharpSyntaxWalker {
    // Contains tokens for methods that represent queries.
    public Dictionary<String, SyntaxToken[]> TokensForQuery
        = new Dictionary<String, SyntaxToken[]>();

    // Contains operators and operands for the query
    // we are currently visiting. These form the basis
    // of computing Halstead's metrics.
    public Dictionary<String, OperatorsAndOperands> OpsForQuery
        = new Dictionary<String, OperatorsAndOperands>();

    // Contains raw (unweighted) node counts and
    // accumulated node weights for our own metrics.
    public Dictionary<String, NodeCountAndWeight> WeightsForQuery
        = new Dictionary<String, NodeCountAndWeight>();

    // The query we are currently visiting.
    private string? CurrentQuery = null;

    // Helper for increasing the unweighted and weighted node counts.
    private void IncrementNodeCount(int weight) {
        if (this.CurrentQuery == null) {
            return;
        }

        var weights = this.WeightsForQuery[this.CurrentQuery];
        weights.NodeCount += 1;
        weights.NodeWeight += weight;
        this.WeightsForQuery[this.CurrentQuery] = weights;
    }

    // Helper for adding a new operator (for Halstead metrics).
    private void AddOperator(string op) {
        if (this.CurrentQuery != null) {
            this.OpsForQuery[this.CurrentQuery].Operators.Add(op);
        }
    }

    // Helper for adding a new operand (for Halstead metrics).
    private void AddOperand(string op) {
        if (this.CurrentQuery != null) {
            this.OpsForQuery[this.CurrentQuery].Operands.Add(op);
        }
    }

    public override void VisitMethodDeclaration(MethodDeclarationSyntax node) {
        string queryPrefix = "Query";
        string methodName = node.Identifier.Text;

        if (methodName.StartsWith(queryPrefix)) {
            // strip prefix
            string queryName = methodName.Substring(queryPrefix.Length).ToSnakeCase();

            // Get all the tokens of the query body. Do not include the declaration,
            // because that is repeated across all queries and it does not contain
            // any useful information.
            this.TokensForQuery[queryName] = node.Body!.DescendantTokens().ToArray();

            // Initialize accumulators for various metrics of this query.
            this.OpsForQuery[queryName] = new OperatorsAndOperands {
                Operators = new List<String>(),
                Operands = new List<String>(),
            };
            this.WeightsForQuery[queryName] = new NodeCountAndWeight {
                NodeCount = 0,
                NodeWeight = 0,
            };

            // Just set this to the query name visited most recently.
            // We do not support nested method declarations.
            this.CurrentQuery = queryName;

            // We also detect recursive queries here. Currently, that is
            // a bit of a hack: we know exactly which query is recursive,
            // so we hard-code it here.
            if (queryName == "SiblingsAndParents") {
                IncrementNodeCount(8); // recursion has a weight of 8 and no separate operator
            }
        } else {
            this.CurrentQuery = null;
            Console.WriteLine("Warning: ignoring non-query method `{0}()`", methodName);
        }

        base.VisitMethodDeclaration(node);
    }

    // The visitor methods below are only for expressions within the
    // body of the current query. Their job is to count operators and
    // operands for Halstead metrics, and weighted or non-weighted
    // nodes for our own tree-based metrics. All other constructs
    // (such as type declarations, visibility modifiers, global
    // variables) are ignored.
    //
    // We also ignore AST nodes that represent duplicates and as such
    // would inflate the metrics. An example of this kind of node is
    // the call-time argument of a function. There is a separate
    // `ArgumentSyntax` class that represents expressions passed to
    // a function; however, this is redundant because the expressions
    // being passed will be visited anyway. Another example is the
    // `case` labels of a `switch` expression. They are not counted
    // separately because the contained pattern or literal will be
    // counted in any case.
    //
    // Nodes related to imperative looping and control flow (e.g.
    // `break`, `continue`, and `try...catch`) are also ignored.
    // They are not present in any of our queries anyway.

    public override void VisitAnonymousObjectCreationExpression(
        AnonymousObjectCreationExpressionSyntax node
    ) {
        IncrementNodeCount(1); // no direct equivalent in SQL
        AddOperator("new");

        base.VisitAnonymousObjectCreationExpression(node);
    }

    public override void VisitAnonymousObjectMemberDeclarator(
        AnonymousObjectMemberDeclaratorSyntax node
    ) {
        // Simple projection with named field (~`AS` alias)
        IncrementNodeCount(1);
        AddOperator("="); // object member assignment/initialization

        base.VisitAnonymousObjectMemberDeclarator(node);
    }

    public override void VisitNameEquals(NameEqualsSyntax node) {
        IncrementNodeCount(1); // simple identifier
        AddOperand(node.Name.Identifier.ValueText);
        // intentionally do NOT call `base` so that identifier is not visited twice
    }

    public override void VisitArrayCreationExpression(ArrayCreationExpressionSyntax node) {
        // Array literal (~a `VALUES` clause)
        IncrementNodeCount(3);
        AddOperator("new[]");

        base.VisitArrayCreationExpression(node);
    }

    public override void VisitAssignmentExpression(AssignmentExpressionSyntax node) {
        IncrementNodeCount(1); // no direct equivalent in SQL, assume weight of 1
        AddOperator("=");

        base.VisitAssignmentExpression(node);
    }

    public override void VisitBinaryExpression(BinaryExpressionSyntax node) {
        int weight = node.OperatorToken.ValueText switch {
            "+"  => 2,
            "-"  => 2,
            "*"  => 2,
            "/"  => 2,
            "==" => 2,
            "!=" => 3,
            "<"  => 3,
            ">"  => 2,
            "<=" => 3,
            ">=" => 3,
            "%"  => 5,
            "as" => 2,
            // there's no direct SQL equivalent for `is`, but it is type-related,
            // and in the SQL queries, type checking is performed using equality.
            // Both `as`/`CAST` and `==` have a weight of 2, hence so does `is`.
            "is" => 2,
            "&&" => 2,
            "||" => 2,
            _    => 1,
        };

        IncrementNodeCount(weight);
        AddOperator(node.OperatorToken.ValueText);

        base.VisitBinaryExpression(node);
    }

    public override void VisitCastExpression(CastExpressionSyntax node) {
        IncrementNodeCount(2);
        AddOperator("as"); // it's not literally an `as` token, but it's semantically equivalent

        base.VisitCastExpression(node);
    }

    public override void VisitConditionalExpression(ConditionalExpressionSyntax node) {
        IncrementNodeCount(2); // has the same weight as a `CASE` expression in SQL
        AddOperator("?:");

        base.VisitConditionalExpression(node);
    }

    public override void VisitConstantPattern(ConstantPatternSyntax node) {
        IncrementNodeCount(1); // a literal
        AddOperand(node.Expression.ToString());

        base.VisitConstantPattern(node);
    }

    public override void VisitElementAccessExpression(ElementAccessExpressionSyntax node) {
        // Member lookup, only used for substituting arguments, so nothing complex.
        IncrementNodeCount(1);
        AddOperator("[]");

        base.VisitElementAccessExpression(node);
    }

    public override void VisitElseClause(ElseClauseSyntax node) {
        // We have already used a weight of 2 for the `if` statement
        // (since it's the rough equivalent of the `CASE` statement
        // in SQL). And the "else" arm of all conditional expressions
        // just gets unity weight anyway.
        IncrementNodeCount(1);
        AddOperator("else");

        base.VisitElseClause(node);
    }

    public override void VisitEmptyStatement(EmptyStatementSyntax node) {
        IncrementNodeCount(1);
        AddOperator(";");

        base.VisitEmptyStatement(node);
    }

    public override void VisitExpressionStatement(ExpressionStatementSyntax node) {
        IncrementNodeCount(1);
        AddOperator(";");

        base.VisitExpressionStatement(node);
    }

    public override void VisitFromClause(FromClauseSyntax node) {
        IncrementNodeCount(2); // direct equivalent of SQL `FROM` clause
        AddOperator("from");

        base.VisitFromClause(node);
    }

    public override void VisitGenericName(GenericNameSyntax node) {
        // No equivalent to generics in SQL -- assume a weight of 1.
        // Also, generics are basically type-level functions or operators.
        IncrementNodeCount(1);
        AddOperator("T<_>");

        base.VisitGenericName(node);
    }

    public override void VisitGroupClause(GroupClauseSyntax node) {
        IncrementNodeCount(2); // direct equivalent of SQL `GROUP BY` clause
        AddOperator("group");

        base.VisitGroupClause(node);
    }

    public override void VisitIdentifierName(IdentifierNameSyntax node) {
        IncrementNodeCount(1); // simple identifier
        AddOperand(node.Identifier.ValueText);

        base.VisitIdentifierName(node);
    }

    public override void VisitIfStatement(IfStatementSyntax node) {
        // All conditionals get the same weight as the `CASE` clause in SQL.
        IncrementNodeCount(2);
        AddOperator("if");

        base.VisitIfStatement(node);
    }

    public override void VisitInvocationExpression(InvocationExpressionSyntax node) {
        // Regular SQL functions get a weight of 1. However, some operations
        // that have their own, special syntax in SQL are expressed using
        // plain method calls in LINQ. Therefore, we must check the callee
        // of invocation expressions in order to see if it matches one of
        // these special constructs/operators.
        switch (node.Expression) {
            case MemberAccessExpressionSyntax memberAccess:
                string op = memberAccess.Name.Identifier.ValueText;
                int weight = op switch {
                    "Union"     => 2,
                    "Intersect" => 8,
                    "Except"    => 8,
                    "Any"       => 3, // ~`EXISTS`
                    "Take"      => 2, // ~`LIMIT`
                    _           => 1,
                };

                // The 1-weight simple function calls don't count as separate operators.
                IncrementNodeCount(weight);
                AddOperator(weight > 1 ? op : "call()");
                break;

            default:
                // Regular function calls get a weight of 1
                IncrementNodeCount(1);
                AddOperator("call()");
                break;
        }

        // For special operators, this will also visit the member access
        // expression once more, but that is disproportionately harder to
        // fix compared to how little benefit it has, so we're OK with it.
        base.VisitInvocationExpression(node);
    }

    public override void VisitJoinClause(JoinClauseSyntax node) {
        IncrementNodeCount(2); // direct equivalent of SQL `INNER JOIN`
        AddOperator("join");

        base.VisitJoinClause(node);
    }

    public override void VisitLetClause(LetClauseSyntax node) {
        // `let` is the approximate equivalent of `WITH`
        // (Common Table Expressions, CTEs) in SQL: a variable
        // binding.
        IncrementNodeCount(3);
        AddOperator("let");

        base.VisitLetClause(node);
    }

    public override void VisitLiteralExpression(LiteralExpressionSyntax node) {
        IncrementNodeCount(1);
        AddOperand(node.Token.ToString());

        base.VisitLiteralExpression(node);
    }

    public override void VisitLocalDeclarationStatement(LocalDeclarationStatementSyntax node) {
        // In our queries, these are always variables.
        // Therefore, they are also equivalent with `let` clauses.
        IncrementNodeCount(3);
        AddOperator("let"); // that's not the keyword, but they are semantically equivalent.

        base.VisitLocalDeclarationStatement(node);
    }

    public override void VisitMemberAccessExpression(MemberAccessExpressionSyntax node) {
        // Member accesses are simple names, so they have a weight of 1.
        IncrementNodeCount(1);
        AddOperator(".");

        base.VisitMemberAccessExpression(node);
    }

    public override void VisitNullableType(NullableTypeSyntax node) {
        IncrementNodeCount(1);
        AddOperator("T?");

        base.VisitNullableType(node);
    }

    public override void VisitOrderByClause(OrderByClauseSyntax node) {
        IncrementNodeCount(2); // Direct equivalent of SQL's `ORDER BY` clause
        AddOperator("orderby");

        base.VisitOrderByClause(node);
    }

    // No separate visitor for `OrderingSyntax` -- we don't care about
    // the ascending/descending direction, it's got the same complexity

    public override void VisitParenthesizedExpression(ParenthesizedExpressionSyntax node) {
        IncrementNodeCount(1); // very common indeed
        AddOperator("()");

        base.VisitParenthesizedExpression(node);
    }

    public override void VisitPostfixUnaryExpression(PostfixUnaryExpressionSyntax node) {
        IncrementNodeCount(1); // only the `!` operator, which has no SQL equivalent
        AddOperator(node.OperatorToken.ValueText);

        base.VisitPostfixUnaryExpression(node);
    }

    public override void VisitPredefinedType(PredefinedTypeSyntax node) {
        IncrementNodeCount(1); // Predefined types are simple identifiers.
        AddOperand(node.Keyword.ValueText);

        base.VisitPredefinedType(node);
    }

    public override void VisitPrefixUnaryExpression(PrefixUnaryExpressionSyntax node) {
        string op = node.OperatorToken.ValueText;
        int weight = op switch {
            "+" => 2,
            "-" => 2,
            "!" => 2,
            _   => 1,
        };

        IncrementNodeCount(weight);
        AddOperator(op);

        base.VisitPrefixUnaryExpression(node);
    }

    // There is no separate visitor for query expressions,
    // since we don't just want to gratuitously add 1
    // to the weight of every query, that is redundant.

    public override void VisitReturnStatement(ReturnStatementSyntax node) {
        IncrementNodeCount(1); // no equivalent in SQL
        AddOperator("return");

        base.VisitReturnStatement(node);
    }

    public override void VisitSelectClause(SelectClauseSyntax node) {
        IncrementNodeCount(2); // direct equivalent of the SQL `SELECT` clause
        AddOperator("select");

        base.VisitSelectClause(node);
    }

    public override void VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node) {
        IncrementNodeCount(1); // only used for predicates
        AddOperator("=>");

        base.VisitSimpleLambdaExpression(node);
    }

    public override void VisitWhereClause(WhereClauseSyntax node) {
        IncrementNodeCount(2); // direct equivalent of SQL's `WHERE` clause
        AddOperator("where");

        base.VisitWhereClause(node);
    }
}

// Used for computing Halstead's metrics.
struct OperatorsAndOperands {
    public List<String> Operators;
    public List<String> Operands;
}

// Used for computing our own metrics.
struct NodeCountAndWeight {
    public int NodeCount;
    public int NodeWeight;
}

// The actual Halstead metrics.
struct HalsteadMetrics {
    public int Vocabulary;
    public int Length;
    public double EstimatedLength;
    public double Volume;
    public double Difficulty;
    public double Effort;
}

}
