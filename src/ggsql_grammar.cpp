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

		// Aesthetic may carry a `:type` suffix (e.g. `color:nominal`) — split it.
		string aesth_token = tokens[aesth_idx];
		size_t colon = aesth_token.find(':');
		string type_override;
		if (colon != string::npos) {
			type_override = aesth_token.substr(colon + 1);
			aesth_token = aesth_token.substr(0, colon);
			if (aesth_token.empty()) {
				result.error = "Empty aesthetic name before ':'";
				return result;
			}
			if (type_override != "quantitative" && type_override != "ordinal" &&
			    type_override != "nominal" && type_override != "temporal") {
				result.error =
				    "Invalid aesthetic type '" + type_override +
				    "' (expected quantitative, ordinal, nominal, or temporal)";
				return result;
			}
		}

		AestheticMapping mapping;
		mapping.expression = std::move(expr);
		mapping.aesthetic = aesth_token;
		result.stmt.aesthetics.push_back(std::move(mapping));
		if (!type_override.empty()) {
			TypeOverride ov;
			ov.aesthetic = aesth_token;
			ov.type = std::move(type_override);
			result.stmt.type_overrides.push_back(std::move(ov));
		}
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

	while (!at_end() && !IEqual(peek(), "FACET") && !IEqual(peek(), "SCALE")) {
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
		if (at_end() || IEqual(peek(), "SCALE") || IEqual(peek(), "ROWS") ||
		    IEqual(peek(), "COLS")) {
			result.error = "Expected expression after 'FACET BY'";
			return result;
		}
		string expr;
		while (!at_end() && !IEqual(peek(), "SCALE") && !IEqual(peek(), "ROWS") &&
		       !IEqual(peek(), "COLS")) {
			if (!expr.empty()) {
				expr += " ";
			}
			expr += tokens[i++];
		}
		AestheticMapping facet;
		facet.expression = std::move(expr);
		facet.aesthetic = "facet";
		result.stmt.aesthetics.push_back(std::move(facet));

		// Optional layout: ROWS (vertical stack), COLS (horizontal stack), or
		// nothing (grid — Vega-Lite default).
		if (!at_end() && (IEqual(peek(), "ROWS") || IEqual(peek(), "COLS"))) {
			result.stmt.facet_layout = StringUtil::Lower(tokens[i]);
			i++;
		}
	}

	// Optional `SCALE <aesthetic> <op> <args...>` clauses, zero or more.
	// Supported ops:
	//   TO <scheme>             — color scheme name
	//   ZERO <true|false>       — include zero in quantitative scale
	//   DOMAIN <num1> <num2>    — explicit numeric domain
	while (!at_end() && IEqual(peek(), "SCALE")) {
		i++;
		if (at_end()) {
			result.error = "Expected aesthetic name after 'SCALE'";
			return result;
		}
		ScaleSpec scale;
		scale.aesthetic = tokens[i++];
		if (at_end()) {
			result.error = "Expected SCALE operator (TO / ZERO / DOMAIN) after aesthetic";
			return result;
		}
		string op = tokens[i++];
		if (IEqual(op, "TO")) {
			if (at_end()) {
				result.error = "Expected scheme name after 'SCALE <aesthetic> TO'";
				return result;
			}
			scale.property = "scheme";
			scale.value_json = "\"" + tokens[i++] + "\"";
		} else if (IEqual(op, "ZERO")) {
			if (at_end()) {
				result.error = "Expected boolean (true/false) after 'SCALE <aesthetic> ZERO'";
				return result;
			}
			string v = tokens[i++];
			if (IEqual(v, "true")) {
				scale.value_json = "true";
			} else if (IEqual(v, "false")) {
				scale.value_json = "false";
			} else {
				result.error = "Expected 'true' or 'false' after 'SCALE <aesthetic> ZERO', got '" +
				               v + "'";
				return result;
			}
			scale.property = "zero";
		} else if (IEqual(op, "DOMAIN")) {
			if (at_end()) {
				result.error = "Expected two numeric bounds after 'SCALE <aesthetic> DOMAIN'";
				return result;
			}
			string lo = tokens[i++];
			if (at_end()) {
				result.error = "Expected upper bound after 'SCALE <aesthetic> DOMAIN <lo>'";
				return result;
			}
			string hi = tokens[i++];
			scale.property = "domain";
			scale.value_json = "[" + lo + "," + hi + "]";
		} else {
			result.error = "Unknown SCALE operator '" + op +
			               "' (expected TO / ZERO / DOMAIN)";
			return result;
		}
		result.stmt.scales.push_back(std::move(scale));
	}

	if (!at_end()) {
		result.error = "Unexpected token '" + tokens[i] + "' after VISUALIZE clause";
		return result;
	}

	result.success = true;
	return result;
}

} // namespace ggsql
} // namespace duckdb
