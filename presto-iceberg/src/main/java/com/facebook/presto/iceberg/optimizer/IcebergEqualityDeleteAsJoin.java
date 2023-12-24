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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.ColumnIdentity;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergMetadataColumn;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.TableType;
import com.facebook.presto.iceberg.TypeConverter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.ConnectorJoinNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.iceberg.IcebergColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.iceberg.IcebergColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.DATA_SEQUENCE_NUMBER;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isDeleteToJoinPushdownEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public class IcebergEqualityDeleteAsJoin
        implements ConnectorPlanOptimizer
{
    private final StandardFunctionResolution functionResolution;
    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;

    IcebergEqualityDeleteAsJoin(StandardFunctionResolution functionResolution,
            IcebergTransactionManager transactionManager,
            TypeManager typeManager)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isDeleteToJoinPushdownEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new DeleteAsJoinRewriter(functionResolution,
                transactionManager, idAllocator, session, typeManager, variableAllocator), maxSubplan);
    }

    private static class DeleteAsJoinRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager transactionManager;
        private final TypeManager typeManager;
        private final VariableAllocator variableAllocator;

        public DeleteAsJoinRewriter(
                StandardFunctionResolution functionResolution,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session,
                TypeManager typeManager,
                VariableAllocator variableAllocator)
        {
            this.functionResolution = functionResolution;
            this.transactionManager = transactionManager;
            this.idAllocator = idAllocator;
            this.session = session;
            this.typeManager = typeManager;
            this.variableAllocator = variableAllocator;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle table = node.getTable();
            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table.getConnectorHandle();
            IcebergTableName tableName = icebergTableHandle.getTableName();
            if (!tableName.getSnapshotId().isPresent() || tableName.getTableType() != TableType.DATA) {
                // Node is already optimized or not ready for planning
                return node;
            }

            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(table.getTransaction());
            Table icebergTable = getIcebergTable(metadata, session, icebergTableHandle.getSchemaTableName());
            HashSet<Set<Integer>> deleteSchemas = new HashSet<>();
            Map<Integer, PartitionFieldInfo> partitionFields = new HashMap<>();
            try (CloseableIterator<DeleteFile> files =
                    IcebergUtil.getDeleteFiles(icebergTable, tableName.getSnapshotId().get(), icebergTableHandle.getPredicate()).iterator()) {
                while (files.hasNext()) {
                    DeleteFile delete = files.next();
                    if (delete.content() == FileContent.EQUALITY_DELETES) {
                        PartitionSpec partitionSpec = icebergTable.specs().get(delete.specId());
                        Types.StructType partitionType = partitionSpec.partitionType();
                        partitionSpec.fields().forEach(f -> partitionFields.put(f.fieldId(), new PartitionFieldInfo(partitionType.field(f.fieldId()), f)));
                        List<Integer> partitionFieldIds = partitionSpec.fields().stream().map(PartitionField::fieldId).collect(Collectors.toList());
                        HashSet<Integer> result = new HashSet<>();
                        result.addAll(delete.equalityFieldIds());
                        result.addAll(partitionFieldIds);
                        deleteSchemas.add(ImmutableSet.copyOf(result));
                    }
                }
            }
            catch (IOException e) {
                return node;
            }

            if (deleteSchemas.isEmpty()) {
                // no equality deletes
                return node;
            }

            // Add all the fields required by the join that were not added by the user's query
            ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments =
                    createAssignmentsForUnselectedFields(node, deleteSchemas, partitionFields, icebergTable);

            TableScanNode updatedTableScan = createNewRoot(node, icebergTableHandle, tableName, unselectedAssignments, table);
            Map<Integer, VariableReferenceExpression> reverseAssignmentsMap =
                    updatedTableScan.getAssignments().entrySet().stream().collect(Collectors.toMap(x -> ((IcebergColumnHandle) (x.getValue())).getId(), Map.Entry::getKey));

            List<RowExpression> deleteVersionColumns = new ArrayList<>();
            PlanNode parentJoin = updatedTableScan;
            for (Set<Integer> deleteSchema : deleteSchemas) {
                List<Types.NestedField> deleteFields = deleteSchema
                        .stream()
                        .map(fieldId -> icebergTable.schema().findField(fieldId))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                VariableReferenceExpression joinSequenceNumber = toVariableReference(IcebergColumnHandle.dataSequenceNumberColumnHandle());
                deleteVersionColumns.add(joinSequenceNumber);
                ImmutableMap<VariableReferenceExpression, ColumnHandle> deleteColumnAssignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .putAll(deleteFields.stream().collect(Collectors.toMap(this::toVariableReference, this::toIcebergColumnHandle)))
                        .put(joinSequenceNumber, IcebergColumnHandle.dataSequenceNumberColumnHandle())
                        .build();
                Set<ConnectorJoinNode.EquiJoinClause> clauses = deleteColumnAssignments.entrySet().stream()
                        .filter(f -> !IcebergMetadataColumn.isMetadataColumnId(((IcebergColumnHandle) (f.getValue())).getId()))
                        .map(kvp -> {
                            VariableReferenceExpression left = reverseAssignmentsMap.get(((IcebergColumnHandle) (kvp.getValue())).getId());
                            VariableReferenceExpression right = kvp.getKey();
                            return new ConnectorJoinNode.EquiJoinClause(left, right);
                        }).collect(Collectors.toSet());
                VariableReferenceExpression sourceSequenceNumber = reverseAssignmentsMap.get(DATA_SEQUENCE_NUMBER.getId());
                FunctionHandle lessThan = functionResolution.comparisonFunction(OperatorType.LESS_THAN, BigintType.BIGINT, BigintType.BIGINT);
                RowExpression versionFilter = new CallExpression(lessThan.getName(),
                        lessThan,
                        BooleanType.BOOLEAN,
                        Collections.unmodifiableList(Arrays.asList(sourceSequenceNumber, joinSequenceNumber)));

                List<VariableReferenceExpression> outputs = deleteColumnAssignments.keySet().asList();
                IcebergTableHandle deletesTableHanlde = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                        new IcebergTableName(tableName.getTableName(), TableType.EQUALITY_DELETES, tableName.getSnapshotId(), Optional.empty()),
                        icebergTableHandle.isSnapshotSpecified(),
                        icebergTableHandle.getPredicate(),
                        Optional.of(SchemaParser.toJson(new Schema(deleteFields))));
                TableScanNode deleteTableScan = new TableScanNode(Optional.empty(),
                        idAllocator.getNextId(),
                        new TableHandle(table.getConnectorId(), deletesTableHanlde, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                        outputs,
                        deleteColumnAssignments,
                        node.getCurrentConstraint(),
                        node.getEnforcedConstraint());

                parentJoin = new ConnectorJoinNode(idAllocator.getNextId(),
                        Arrays.asList(parentJoin, deleteTableScan),
                        Optional.empty(),
                        ConnectorJoinNode.Type.LEFT,
                        clauses,
                        Sets.newHashSet(versionFilter),
                        Stream.concat(parentJoin.getOutputVariables().stream(), deleteTableScan.getOutputVariables().stream()).collect(Collectors.toList()));
            }

            FilterNode filter = new FilterNode(Optional.empty(), idAllocator.getNextId(), Optional.empty(), parentJoin,
                    new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL, BooleanType.BOOLEAN,
                            new SpecialFormExpression(SpecialFormExpression.Form.COALESCE, BigintType.BIGINT, deleteVersionColumns)));

            return filter;
        }

        /**
         * - Updates table handle to DATA_WITHOUT_EQUALITY_DELETES since the page source for this node should now not apply equality deletes.
         * - Adds extra assignments and outputs that are needed by the join
         */
        private TableScanNode createNewRoot(TableScanNode node, IcebergTableHandle icebergTableHandle, IcebergTableName tableName, ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments, TableHandle table)
        {
            IcebergTableHandle updatedHandle = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                    new IcebergTableName(tableName.getTableName(), TableType.DATE_WITHOUT_EQUALITY_DELETES, tableName.getSnapshotId(), tableName.getChangelogEndSnapshot()),
                    icebergTableHandle.isSnapshotSpecified(),
                    icebergTableHandle.getPredicate(),
                    icebergTableHandle.getTableSchemaJson());

            VariableReferenceExpression dataSequenceNumberVariableReference = toVariableReference(IcebergColumnHandle.dataSequenceNumberColumnHandle());
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignmentsBuilder = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                    .put(dataSequenceNumberVariableReference, IcebergColumnHandle.dataSequenceNumberColumnHandle())
                    .putAll(unselectedAssignments)
                    .putAll(node.getAssignments());
            ImmutableList.Builder<VariableReferenceExpression> outputsBuilder = ImmutableList.builder();
            outputsBuilder.addAll(node.getOutputVariables());
            if (!node.getAssignments().containsKey(dataSequenceNumberVariableReference)) {
                outputsBuilder.add(dataSequenceNumberVariableReference);
            }
            outputsBuilder.addAll(ImmutableList.copyOf(unselectedAssignments.keySet()));

            return new TableScanNode(node.getSourceLocation(),
                    node.getId(),
                    Optional.of(node),
                    new TableHandle(table.getConnectorId(), updatedHandle, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                    outputsBuilder.build(),
                    assignmentsBuilder.build(),
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
        }

        private ImmutableMap<VariableReferenceExpression, ColumnHandle> createAssignmentsForUnselectedFields(TableScanNode node,
                HashSet<Set<Integer>> deleteSchemas,
                Map<Integer, PartitionFieldInfo> partitionFields,
                Table icebergTable)
        {
            Set<Integer> selectedFields = node.getAssignments().values().stream().map(f -> ((IcebergColumnHandle) f).getId()).collect(Collectors.toSet());
            Set<Integer> unselectedFields = Sets.difference(deleteSchemas.stream().reduce(Sets::union).orElseGet(Collections::emptySet), selectedFields);
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> unselectedAssignmentsBuilder = ImmutableMap.builder();
            unselectedFields
                    .forEach(fieldId -> {
                        if (partitionFields.containsKey(fieldId)) {
                            PartitionFieldInfo partitionFieldInfo = partitionFields.get(fieldId);
                            Types.NestedField sourceField = icebergTable.schema().findField(partitionFieldInfo.getPartitionField().sourceId());
                            Type partitionFieldType = TypeConverter.toPrestoType(partitionFieldInfo.getPartitionField().transform().getResultType(sourceField.type()), typeManager);
                            unselectedAssignmentsBuilder.put(
                                    variableAllocator.newVariable(partitionFieldInfo.getPartitionField().name(), partitionFieldType),
                                    IcebergColumnHandle.create(partitionFieldInfo.getNestedField(), typeManager, PARTITION_KEY));
                        }
                        else {
                            Types.NestedField schemaField = icebergTable.schema().findField(fieldId);
                            unselectedAssignmentsBuilder.put(
                                    variableAllocator.newVariable(schemaField.name(), TypeConverter.toPrestoType(schemaField.type(), typeManager)),
                                    IcebergColumnHandle.create(schemaField, typeManager, REGULAR));
                        }
                    });
            return unselectedAssignmentsBuilder.build();
        }

        private VariableReferenceExpression toVariableReference(IcebergColumnHandle c)
        {
            return variableAllocator.newVariable(c.getName(), c.getType());
        }

        private IcebergColumnHandle toIcebergColumnHandle(Types.NestedField f)
        {
            return new IcebergColumnHandle(toColumnIdentity(f), TypeConverter.toPrestoType(f.type(), typeManager), Optional.empty(), REGULAR);
        }

        private VariableReferenceExpression toVariableReference(Types.NestedField field)
        {
            return variableAllocator.newVariable(field.name(), TypeConverter.toPrestoType(field.type(), typeManager));
        }

        private static ColumnIdentity toColumnIdentity(Types.NestedField nestedField)
        {
            return new ColumnIdentity(nestedField.fieldId(), nestedField.name(), ColumnIdentity.TypeCategory.PRIMITIVE, Collections.emptyList());
        }

        private static class PartitionFieldInfo
        {
            private final Types.NestedField nestedField;
            private final PartitionField partitionField;

            private PartitionFieldInfo(Types.NestedField nestedField, PartitionField partitionField)
            {
                this.nestedField = nestedField;
                this.partitionField = partitionField;
            }

            public PartitionField getPartitionField()
            {
                return partitionField;
            }

            public Types.NestedField getNestedField()
            {
                return nestedField;
            }
        }
    }
}
