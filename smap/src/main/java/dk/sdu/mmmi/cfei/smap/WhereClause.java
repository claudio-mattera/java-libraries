package dk.sdu.mmmi.cfei.smap;

import fj.data.Either;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class WhereClause {

    private WhereClause(Either<List<Clause>, String> content) {
        this.content = content;
    }

    static WhereClause empty() {
        return new WhereClause(Either.right(""));
    }

    WhereClause whereIs(String key, String value) {
        return where(new Clause(key, "=", value));
    }

    WhereClause whereLike(String key, String value) {
        return where(new Clause(key, "like", value));
    }

    WhereClause whereHas(String key, String value) {
        return where(new Clause(key, "has", value));
    }

    WhereClause whereRegexp(String key, String value) {
        return where(new Clause(key, "~", value));
    }

    WhereClause whereRaw(String raw) {
        return this.content.either(
                originalClauses -> {
                    // Warning, discarding old clauses
                    return new WhereClause(Either.right(raw));
                },
                originalRaw -> {
                    return new WhereClause(Either.right(raw));
                }
        );
    }

    private WhereClause where(Clause clause) {
        return this.content.either(
                originalClauses -> {
                    final List<Clause> clauses = new ArrayList<>(originalClauses);
                    clauses.add(clause);
                    return new WhereClause(Either.left(clauses));
                },
                originalRaw -> {
                    // Warning, discarding old raw clause
                    final List<Clause> clauses = new ArrayList<>();
                    clauses.add(clause);
                    return new WhereClause(Either.left(clauses));
                }
        );
    }

    @Override
    public String toString() {
        return content.either(
                clauses -> clauses.stream()
                .map(c -> c.toString())
                .collect(Collectors.joining(" and ")),
                raw -> raw
        );
    }

    private final Either<List<Clause>, String> content;

    class Clause {

        public Clause(String key, String operator, String value) {
            this.key = key;
            this.operator = operator;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + ' ' + operator + " '" + value + '\'';
        }

        public final String key;
        public final String operator;
        public final String value;
    }
}
