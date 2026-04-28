#include "ggsql_grammar.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace ggsql {

namespace {

struct TokenizeResult {
	vector<string> tokens;
	string error;
};

// Tokenizer: at depth 0, splits on whitespace and emits commas as their own
// token. Inside parens or quotes (single or double) all characters are
// accumulated into the current token so that `coalesce(a, b)`, `"my col"`,
// `'don''t'`, and `(SELECT max(x) FROM t)` all become single tokens. SQL
// doubled-quote escapes (`''` and `""`) are handled. Trailing semicolons are
// dropped.
TokenizeResult Tokenize(const string &input) {
	TokenizeResult out;
	auto &tokens = out.tokens;
	string current;
	int paren_depth = 0;
	char quote_char = 0;

	auto flush = [&]() {
		if (!current.empty()) {
			tokens.push_back(std::move(current));
			current.clear();
		}
	};

	for (size_t i = 0; i < input.size(); i++) {
		char c = input[i];

		if (quote_char != 0) {
			current += c;
			if (c == quote_char) {
				if (i + 1 < input.size() && input[i + 1] == quote_char) {
					current += input[++i]; // SQL '' / "" escape
				} else {
					quote_char = 0;
				}
			}
			continue;
		}

		if (c == '\'' || c == '"') {
			current += c;
			quote_char = c;
			continue;
		}

		if (paren_depth > 0) {
			if (c == '(') {
				paren_depth++;
			} else if (c == ')') {
				paren_depth--;
			}
			current += c;
			continue;
		}

		// Outside parens, outside quotes.
		if (c == '(') {
			paren_depth++;
			current += c;
		} else if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
			flush();
		} else if (c == ',') {
			flush();
			tokens.emplace_back(",");
		} else if (c == ';') {
			flush();
		} else {
			current += c;
		}
	}
	flush();

	if (quote_char != 0) {
		out.error = string("Unterminated string literal or quoted identifier (missing closing ") +
		            quote_char + ")";
	} else if (paren_depth != 0) {
		out.error = "Unbalanced parentheses in input";
	}
	return out;
}

bool IEqual(const string &tok, const char *kw) {
	return StringUtil::CIEquals(tok, kw);
}

} // namespace

ParseResult ParseGgsql(const string &query) {
	ParseResult result;
	auto tokenized = Tokenize(query);
	if (!tokenized.error.empty()) {
		result.error = tokenized.error;
		return result;
	}
	auto &tokens = tokenized.tokens;
	size_t i = 0;

	auto at_end = [&]() { return i >= tokens.size(); };
	auto peek = [&]() -> const string & {
		static const string empty;
		return at_end() ? empty : tokens[i];
	};
	auto consume = [&](const char *kw) -> bool {
		if (at_end() || !IEqual(tokens[i], kw)) {
			result.error = string("Expected '") + kw + "', got " +
			               (at_end() ? "end of input" : "'" + tokens[i] + "'");
			return false;
		}
		i++;
		return true;
	};

	if (!consume("VISUALIZE")) {
		return result;
	}

	// mapping_list := <expr> AS <ident> (',' <expr> AS <ident>)*
	//
	// <expr> may span multiple tokens (e.g. `bill_len * 2`, `log(bill_dep)`).
	// Boundary scan: each mapping consumes tokens until the next top-level `,`,
	// `FROM`, or `DRAW`. The last token in that range is the aesthetic name; the
	// token immediately preceding it must be `AS`; everything earlier is the
	// expression text (concatenated with single spaces — whitespace inside
	// parens is preserved by DuckDB's downstream parser).
	//
	// Limitations (deferred to a paren-aware tokenizer in a later phase):
	//   - top-level commas inside expressions break the scan (`coalesce(a, b)`)
	//   - `FROM` / `DRAW` keywords inside subqueries break the scan
	//   - quoted identifiers `"my col"` are not supported
	while (true) {
		// Scan forward to the next mapping boundary at depth 0.
		size_t boundary = i;
		while (boundary < tokens.size()) {
			const string &t = tokens[boundary];
			if (t == "," || IEqual(t, "FROM") || IEqual(t, "DRAW")) {
				break;
			}
			boundary++;
		}

		if (boundary == i) {
			result.error = "Expected expression after VISUALIZE (or ',')";
			return result;
		}

		// `… AS` with nothing after the AS.
		if (IEqual(tokens[boundary - 1], "AS")) {
			result.error = "Expected aesthetic name after 'AS'";
			return result;
		}

		// Need at least 3 tokens: <something> AS <aesth>. With 2 we either have
		// no AS at all, or a leading AS with empty expression.
		if (boundary - i < 3) {
			if (boundary - i == 2 && IEqual(tokens[i], "AS")) {
				result.error = "Empty expression before 'AS'";
			} else {
				result.error = "Expected 'AS' before aesthetic '" + tokens[boundary - 1] + "'";
			}
			return result;
		}

		size_t aesth_idx = boundary - 1;
		size_t as_idx = boundary - 2;
		if (!IEqual(tokens[as_idx], "AS")) {
			result.error = "Expected 'AS' before aesthetic '" + tokens[aesth_idx] + "'";
			return result;
		}

		string expr;
		for (size_t k = i; k < as_idx; k++) {
			if (!expr.empty()) {
				expr += " ";
			}
			expr += tokens[k];
		}

		AestheticMapping mapping;
		mapping.expression = std::move(expr);
		mapping.aesthetic = tokens[aesth_idx];
		result.stmt.aesthetics.push_back(std::move(mapping));
		i = boundary;

		if (!at_end() && peek() == ",") {
			i++;
			continue;
		}
		break;
	}

	if (!consume("FROM")) {
		return result;
	}
	if (at_end()) {
		result.error = "Expected table name after 'FROM'";
		return result;
	}
	result.stmt.from_table = tokens[i++];

	while (!at_end() && !IEqual(peek(), "FACET")) {
		if (!consume("DRAW")) {
			return result;
		}
		if (at_end()) {
			result.error = "Expected mark name after 'DRAW'";
			return result;
		}
		DrawLayer layer;
		layer.mark = tokens[i++];
		result.stmt.layers.push_back(std::move(layer));
	}

	if (result.stmt.layers.empty()) {
		result.error = "At least one 'DRAW <mark>' clause is required";
		return result;
	}

	// Optional `FACET BY <expr>`. Stored as a synthetic aesthetic named "facet";
	// "facet" is reserved — Compile() detects it to wrap the spec in a Vega-Lite
	// facet operator and BuildProjectedSql carries it through.
	if (!at_end() && IEqual(peek(), "FACET")) {
		i++; // consume FACET (case-insensitive — already verified)
		if (!consume("BY")) {
			return result;
		}
		if (at_end()) {
			result.error = "Expected expression after 'FACET BY'";
			return result;
		}
		string expr;
		while (!at_end()) {
			if (!expr.empty()) {
				expr += " ";
			}
			expr += tokens[i++];
		}
		AestheticMapping facet;
		facet.expression = std::move(expr);
		facet.aesthetic = "facet";
		result.stmt.aesthetics.push_back(std::move(facet));
	}

	result.success = true;
	return result;
}

} // namespace ggsql
} // namespace duckdb
