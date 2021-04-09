import re
import numpy as np

from collections import namedtuple, Counter
from scipy.stats import entropy


Token = namedtuple('Token', ['ty', 'value'])

TOKEN_REGEXES = {
    'space': re.compile(r'(\s+)'),
    'keyword': re.compile('(true|false|null)'),
    'number': re.compile(r'(-?[0-9]+(\.[0-9]+)?([eE][\+-]?[0-9]+)?)'),
    'string': re.compile(r'"(([^"\\]|\\[\\/"bfnrt]|\\u[0-9a-fA-F]{4})*)"'),
    'comma': re.compile('(,)'),
    'colon': re.compile('(:)'),
    'lbrace': re.compile(r'(\{)'),
    'rbrace': re.compile(r'(\})'),
    'lbracket': re.compile(r'(\[)'),
    'rbracket': re.compile(r'(\])'),
}

KEYWORDS = {
    'from',
    'startWith',
    'connectFromField',
    'connectToField',
    'as',
    'input',
    'as',
    'in',
    'from',
    'localField',
    'foreignField',
    'coll',
    'pipeline',
    'input',
    'cond',
    'branches',
    'case',
    'then',
    'path'
    'preserveNullAndEmptyArrays',
}

# Simple tokenizer that works for the BSON/JSON
# strings corresponding to our queries.
def bson_tokenize(string, ignore_whitespace=True):
    pos = 0

    while pos < len(string):
        matched = False

        for ty, rx in TOKEN_REGEXES.items():
            m = rx.match(string, pos)

            if m:
                pos = m.end()
                matched = True

                if ty != 'space' or not ignore_whitespace:
                    yield Token(ty=ty, value=m[1])

                break

        if not matched:
            raise ValueError(
                'JSON syntax error at character {} near `{}`'.format(pos, string[pos:pos+10])
            )

# Count the number of non-whitespace tokens in a BSON/JSON string.
def token_count(tokens):
    return sum(token.ty != 'space' for token in tokens)

# Since MongoDB queries are written directly as JSON
# or BSON, all operators, keywords, identifiers, and
# string literals are expressed using string literals.
# Therefore, it's not trivial to differentiate between
# these different kinds of tokens. We observe a couple
# of general patterns in order to tell them apart:
#
#   * String literals that begin with `$$` are known to
#     stand for variables, i.e. they are identifiers.
#   * String literals beginning with a single `$` in key
#     position (i.e. when immediately followed by a `:`)
#     represent an operator, in which case they are the
#     equivalent of a keyword in SQL or other languages.
#   * String literals beginning with a single `$` in
#     value position (i.e. when not immediately followed
#     by a `:`) represent an identifier (document field).
#   * String literals not beginning with any `$` at all
#     in key position are either identifiers or keywords.
#     We resolve this ambiguity by recognizing the subset
#     of MongoDB DSL keywords used in our queries.
#   * String literals not beginning with a `$` in value
#     position are always considered string literals.
def normalize_token(token, next_token=None):
    assert token.ty != 'space'

    if token.ty == 'string':
        # a string is never the last token, so this is safe
        iskey = next_token.value == ':'

        if token.value.startswith('$$'):
            newtype = 'ident'
        elif token.value.startswith('$'):
            newtype = 'keyword' if iskey else 'ident'
        else:
            if iskey:
                newtype = 'keyword' if token.value in KEYWORDS else 'ident'
            else:
                newtype = 'string'

        return Token(ty=newtype, value=token.value)

    else:
        return token

# Compute the entropy of tokens in a BSON/JSON string.
def token_entropy(tokens):
    # `tokens[i:i+2]` grabs the i-th token,
    # and if exists, the following one as well.
    counts = Counter(
        normalize_token(*tokens[i:i+2]) for i, _ in enumerate(tokens)
    )

    # Extract frequencies and compute entropy.
    # `scipy.stats.entropy()` auto-normalizes the sum to 1.
    freqs = np.fromiter(counts.values(), dtype=float)

    return entropy(freqs)

# Count the number of AST nodes in some parsed JSON.
def node_count(value):
    if isinstance(value, list):
        # +1 for the array itself
        return 1 + sum(node_count(item) for item in value)

    if isinstance(value, dict):
        # +1 for the dict itself
        return 1 + sum(
            node_count(key) + node_count(val) for key, val in value.items()
        )

    # All the simple types are just 1 node: null, bool, number, string
    assert isinstance(value, (type(None), bool, int, float, str))
    return 1

# Sum the weights of AST nodes. Weights are assigned
# based on the weights of the corresponding SQL features.
def weighted_node_count(value):
    if isinstance(value, list):
        return weight(value) + sum(
            weighted_node_count(item) for item in value
        )

    if isinstance(value, dict):
        return weight(value) + sum(
            weighted_node_count(key) + weighted_node_count(val)
            for key, val in value.items()
        )

    assert isinstance(value, (type(None), bool, int, float, str))
    return weight(value)

# Contains the weights of MongoDB operators.
# Comments describe which equivalent SQL construct
# the corresponding weight was derived from.
OP_WEIGHT = {
    '$project':       2, # SELECT
    '$map':           2, # SELECT, but for embedded arrays
    '$addFields':     1, # AS, simple projection
    '$unset':         1, # AS, simple projection (but _removes_ a field)
    '$match':         2, # WHERE
    '$filter':        2, # WHERE, but for embedded arrays
    '$group':         2, # GROUP BY
    '$sort':          2, # ORDER BY

    '$lookup':        3, # LEFT OUTER JOIN
    '$unwind':        3, # LEFT OUTER JOIN (unwind is typically needed for `$lookup`)
    '$graphLookup':   8, # WITH RECURSIVE
    '$unionWith':     2, # UNION
    '$setUnion':      2, # UNION
    '$setDifference': 8, # EXCEPT
    '$switch':        2, # CASE...WHEN...THEN

    '$add':           2, # `+`
    '$subtract':      2, # `-`
    '$multiply':      2, # `*`
    '$divide':        2, # `/`
    '$mod':           5, # `%`, modulo/remainder

    '$eq':            2, # `=`
    '$neq':           3, # `<>`, `!=`
    '$lt':            3, # `<`
    '$lte':           3, # `<=`
    '$gt':            2, # `>`
    '$gte':           3, # `>=`

    '$sum':           2, # SUM
    '$max':           2, # MAX
    '$avg':           3, # AVG
    '$size':          2, # COUNT
}

# Actually computes the weight of a given node.
def weight(value):
    if isinstance(value, dict) and len(value) == 1:
        operator = list(value.keys())[0]

        # Non-operators still have unit weight,
        # which we have to consider, because
        # not every single-key dictionary
        # represents a MongoDB operator.
        return OP_WEIGHT.get(operator, 1)

    else:
        # Everything else has unit weight.
        return 1

def halstead(tokens):
    operators = Counter()
    operands = Counter()

    for i, _ in enumerate(tokens):
        token = normalize_token(*tokens[i:i+2])

        if token.ty == 'keyword':
            # These keywords designate primitive values, which are not operators.
            if token.value in {'true', 'false', 'null'}:
                operands[token] += 1
            else:
                operators[token] += 1

        elif token.ty in {'string', 'number', 'ident'}:
            operands[token] += 1

    eta1 = len(operators) # number of unique operators
    eta2 = len(operands) # number of unique operands
    n1 = sum(operators.values()) # number of total operators
    n2 = sum(operands.values()) # number of total operands

    eta = eta1 + eta2
    n = n1 + n2
    n_hat = eta1 * np.log2(eta1) + eta2 * np.log2(eta2)
    v = n * np.log2(eta)
    d = eta1 / 2 * n2 / eta2
    e = d * v

    return {
        'halstead_vocabulary': eta,
        'halstead_length': n,
        'halstead_estimated_length': n_hat,
        'halstead_volume': v,
        'halstead_difficulty': d,
        'halstead_effort': e,
    }
