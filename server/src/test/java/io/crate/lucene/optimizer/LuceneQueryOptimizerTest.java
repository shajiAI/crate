/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.lucene.optimizer;

import io.crate.expression.operator.Operator;
import io.crate.expression.operator.Operators;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.lucene.GenericFunctionQuery;
import io.crate.lucene.LuceneQueryBuilderTest;
import io.crate.sql.tree.ComparisonExpression;
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class LuceneQueryOptimizerTest extends LuceneQueryBuilderTest {

    @Test
    public void test_any_operator_cast_on_left_reference_is_moved_to_cast_on_literal() throws Exception {
        for (var op : AnyOperators.Type.operatorSymbols()) {
            Query query = convert("name " + op + " any([1, 2, 2])");
            assertThat(query, not(instanceOf(GenericFunctionQuery.class)));

        }
        for (var op : List.of("like", "ilike")) {
            Query query = convert("name " + op + " any([1, 2, 2])");
            assertThat(query, not(instanceOf(GenericFunctionQuery.class)));

            query = convert("name not " + op + " any([1, 2, 2])");
            assertThat(query, not(instanceOf(GenericFunctionQuery.class)));

        }
    }

    @Test
    public void test_any_operator_cast_on_right_reference_is_moved_to_cast_on_literal() throws Exception {
        for(var op : AnyOperators.Type.operatorSymbols()) {
            Query query = convert("1 " + op + " any(d_array)");
            assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
        }
        for (var op : List.of("like", "ilike")) {
            Query query = convert("'foo' " + op + " any(text_array)");
            assertThat(query, not(instanceOf(GenericFunctionQuery.class)));

            query = convert("'foo' not " + op + " any(text_array)");
            assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
        }
    }

    @Test
    public void test_operator_cast_on_reference_is_moved_to_cast_on_literal() {
        for (var op : Operators.COMPARISON_OPERATORS) {
            op = op.replace(Operator.PREFIX, "");
            if (op.equals(ComparisonExpression.Type.CONTAINED_WITHIN.getValue())) {
                var query = convert("addr " + op + " '192.168.0.1/24'");
                assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
            } else {
                var query = convert("x " + op + " 1");
                assertThat(query, not(instanceOf(GenericFunctionQuery.class)));

                query = convert("1 " + op + " x");
                assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
            }
        }
    }

    @Test
    public void test_operator_subscript_on_reference_cast_is_moved_to_literal_cast() {
        var query = convert("ts_array[1] = 1129224512000");
        assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
    }

    @Test
    public void test_operator_cast_on_array_length_with_reference_is_moved_to_literal_cast() {
        var query = convert("array_length(y_array, 1) < 1");
        assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
    }

    @Test
    public void testAnyEqNestedArray() {
        var query = convert("[1, 2, 3] = any(o_array['xs'])");
        assertThat(query, not(instanceOf(GenericFunctionQuery.class)));
    }
}
