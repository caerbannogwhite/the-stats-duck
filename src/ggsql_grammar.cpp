#include "ggsql_grammar.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace ggsql {

namespace {

// Tokenizer: splits on whitespace, emits commas as their own token, drops trailing
// semicolons. Quoted identifiers and string literals are NOT supported in phase 1.
vector<string> Tokenize(const string &input) {
	vector<string> tokens;
	string current;
	auto flush = [&]() {
		if (!current.empty()) {
			tokens.push_back(std::move(current));
			current.clear();
		}
	};
	for (char c : input) {
		if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
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
	return tokens;
}

bool IEqual(const string &tok, const char *kw) {
	return StringUtil::CIEquals(tok, kw);
}

} // namespace

ParseResult ParseGgsql(const string &query) {
	ParseResult result;
	auto tokens = Tokenize(query);
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

	// mapping_list := <ident> AS <ident> (',' <ident> AS <ident>)*
	while (true) {
		if (at_end()) {
			result.error = "Expected expression after VISUALIZE";
			return result;
		}
		if (IEqual(peek(), "FROM") || IEqual(peek(), "DRAW") || peek() == ",") {
			result.error = "Expected expression after VISUALIZE (or ',')";
			return result;
		}
		AestheticMapping mapping;
		mapping.expression = tokens[i++];
		if (!consume("AS")) {
			return result;
		}
		if (at_end()) {
			result.error = "Expected aesthetic name after 'AS'";
			return result;
		}
		mapping.aesthetic = tokens[i++];
		result.stmt.aesthetics.push_back(std::move(mapping));

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

	while (!at_end()) {
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

	result.success = true;
	return result;
}

} // namespace ggsql
} // namespace duckdb
