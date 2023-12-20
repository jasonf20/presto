/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ConnectorJoinNode
        extends PlanNode
{
    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<VariableReferenceExpression> outputVariables;
    private final Optional<RowExpression> filter;
    private final Optional<VariableReferenceExpression> leftHashVariable;
    private final Optional<VariableReferenceExpression> rightHashVariable;
    private final Optional<DistributionType> distributionType;
    private final Map<String, VariableReferenceExpression> dynamicFilters;

    public ConnectorJoinNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            Type type,
            PlanNode left,
            PlanNode right,
            List<EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable,
            Optional<DistributionType> distributionType,
            Map<String, VariableReferenceExpression> dynamicFilters)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputVariables, "outputVariables is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashVariable, "leftHashVariable is null");
        requireNonNull(rightHashVariable, "rightHashVariable is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(dynamicFilters, "dynamicFilters is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = unmodifiableList(criteria);
        this.outputVariables = unmodifiableList(outputVariables);
        this.filter = filter;
        this.leftHashVariable = leftHashVariable;
        this.rightHashVariable = rightHashVariable;
        this.distributionType = distributionType;
        this.dynamicFilters = unmodifiableMap(dynamicFilters);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return unmodifiableList(Arrays.asList(left, right));
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new ConnectorJoinNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), type, newChildren.get(0), newChildren.get(1), criteria, outputVariables, filter, leftHashVariable, rightHashVariable, distributionType, dynamicFilters);
    }

    private static void checkArgument(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new ConnectorJoinNode(getSourceLocation(), getId(), statsEquivalentPlanNode, type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, distributionType, dynamicFilters);
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public boolean mustPartition()
        {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return this == RIGHT || this == FULL;
        }

        public boolean mustReplicate(List<ConnectorJoinNode.EquiJoinClause> criteria)
        {
            // There is nothing to partition on
            return criteria.isEmpty() && (this == INNER || this == LEFT);
        }
    }

    public static class EquiJoinClause
    {
        private final VariableReferenceExpression left;
        private final VariableReferenceExpression right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") VariableReferenceExpression left, @JsonProperty("right") VariableReferenceExpression right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty
        public VariableReferenceExpression getLeft()
        {
            return left;
        }

        @JsonProperty
        public VariableReferenceExpression getRight()
        {
            return right;
        }

        public EquiJoinClause flip()
        {
            return new EquiJoinClause(right, left);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", left, right);
        }
    }
}
