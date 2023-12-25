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
import org.apache.iceberg.types.Type;
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
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * <p>This optimizer implements equality deletes as a join, rather than having each split read the delete files and apply them.
 * This approach significantly enhances performance for equality deletes, as most delete files will apply to most splits,
 * and opening the delete file in each split incurs considerable overhead. Usually, the delete files are relatively small
 * and can be broadcast easily. Each delete file may have a different schema, though typically there will be only a few delete
 * schemas, often just one (the primary key).</p>
 *
 * <p>For example, consider the following query:
 * <code>SELECT * FROM table;</code>
 * With 2 delete schemas: (pk), (orderid), the query will be transformed into:
 * <pre>
 * SELECT "$data_sequence_number", * FROM table
 * LEFT JOIN "table$equality_deletes1" d1 ON left.pk = d1.pk AND left."$data_sequence_number" < d1."$data_sequence_number" -- Find deletes by schema 1
 * LEFT JOIN "table$equality_deletes2" d2 ON left.orderid = d1.orderid AND left."$data_sequence_number" < d2."$data_sequence_number" -- Find deletes by schema 2
 * WHERE COALESCE(d1."$data_sequence_number", d2."data_sequence_number") IS NULL -- None of the delete files had a delete for this row
 * </pre>
 * Note that table$equality_deletes1 and table$equality_deletes2 are different tables, each containing only the delete files with the schema for this join.</p>
 */

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

            // Collect info about each unique delete schema to join by
            ImmutableMap<Set<Integer>, DeleteSetInfo> deleteSchemas = collectDeleteInformation(icebergTable, icebergTableHandle, tableName.getSnapshotId().get());

            if (deleteSchemas.isEmpty()) {
                // no equality deletes
                return node;
            }

            // Add all the fields required by the join that were not added by the user's query
            ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments = createAssignmentsForUnselectedFields(node, deleteSchemas, icebergTable);
            TableScanNode updatedTableScan = createNewRoot(node, icebergTableHandle, tableName, unselectedAssignments, table);

            Map<Integer, VariableReferenceExpression> reverseAssignmentsMap =
                    updatedTableScan.getAssignments().entrySet().stream().collect(Collectors.toMap(x -> ((IcebergColumnHandle) (x.getValue())).getId(), Map.Entry::getKey));

            List<RowExpression> deleteVersionColumns = new ArrayList<>();
            PlanNode parentNode = updatedTableScan;
            // For each unique delete schema add a join that applies those equality deletes
            for (Map.Entry<Set<Integer>, DeleteSetInfo> entry : deleteSchemas.entrySet()) {
                DeleteSetInfo deleteGroupInfo = entry.getValue();

                List<Types.NestedField> deleteFields = deleteGroupInfo
                        .equalityFieldIds
                        .stream()
                        .map(fieldId -> icebergTable.schema().findField(fieldId))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                VariableReferenceExpression joinSequenceNumber = toVariableReference(IcebergColumnHandle.dataSequenceNumberColumnHandle());
                deleteVersionColumns.add(joinSequenceNumber);
                ImmutableMap<VariableReferenceExpression, ColumnHandle> deleteColumnAssignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .putAll(deleteGroupInfo.allFields(icebergTable.schema()).stream().collect(Collectors.toMap(this::toVariableReference, this::toIcebergColumnHandle)))
                        .put(joinSequenceNumber, IcebergColumnHandle.dataSequenceNumberColumnHandle())
                        .build();

                // ON source.delete_column = deletes.delete_column, ...
                Set<ConnectorJoinNode.EquiJoinClause> clauses = deleteColumnAssignments
                        .entrySet()
                        .stream()
                        .filter(f -> !IcebergMetadataColumn.isMetadataColumnId(((IcebergColumnHandle) (f.getValue())).getId()))
                        .map(assignment -> {
                            VariableReferenceExpression left = reverseAssignmentsMap.get(((IcebergColumnHandle) (assignment.getValue())).getId());
                            VariableReferenceExpression right = assignment.getKey();
                            return new ConnectorJoinNode.EquiJoinClause(left, right);
                        }).collect(Collectors.toSet());

                FunctionHandle lessThan = functionResolution.comparisonFunction(OperatorType.LESS_THAN, BigintType.BIGINT, BigintType.BIGINT);

                // AND source.$data_sequence_number < deletes.$data_sequence_number
                RowExpression versionFilter = new CallExpression(lessThan.getName(),
                        lessThan,
                        BooleanType.BOOLEAN,
                        Collections.unmodifiableList(Arrays.asList(reverseAssignmentsMap.get(DATA_SEQUENCE_NUMBER.getId()), joinSequenceNumber)));

                TableScanNode deleteTableScan = createDeletesTableScan(node,
                        deleteColumnAssignments,
                        icebergTableHandle,
                        tableName,
                        deleteFields,
                        table,
                        deleteGroupInfo);

                parentNode = new ConnectorJoinNode(idAllocator.getNextId(),
                        Arrays.asList(parentNode, deleteTableScan),
                        Optional.empty(),
                        ConnectorJoinNode.Type.LEFT,
                        clauses,
                        Sets.newHashSet(versionFilter),
                        Stream.concat(parentNode.getOutputVariables().stream(), deleteTableScan.getOutputVariables().stream()).collect(Collectors.toList()));
            }

            FilterNode filter = new FilterNode(Optional.empty(), idAllocator.getNextId(), Optional.empty(), parentNode,
                    new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL, BooleanType.BOOLEAN,
                            new SpecialFormExpression(SpecialFormExpression.Form.COALESCE, BigintType.BIGINT, deleteVersionColumns)));

            return filter;
        }

        private static ImmutableMap<Set<Integer>, DeleteSetInfo> collectDeleteInformation(Table icebergTable,
                IcebergTableHandle icebergTableHandle,
                long snapshotId)
        {
            HashMap<Set<Integer>, DeleteSetInfo> deleteInformations = new HashMap<>();
            try (CloseableIterator<DeleteFile> files =
                    IcebergUtil.getDeleteFiles(icebergTable, snapshotId, icebergTableHandle.getPredicate(), Optional.empty()).iterator()) {
                while (files.hasNext()) {
                    DeleteFile delete = files.next();
                    if (delete.content() == FileContent.EQUALITY_DELETES) {
                        Map<Integer, PartitionFieldInfo> partitionFields = new HashMap<>();
                        PartitionSpec partitionSpec = icebergTable.specs().get(delete.specId());
                        Types.StructType partitionType = partitionSpec.partitionType();
                        // PartitionField ids are unique across the entire table in v2. We can assume we are in v2 since v1 doesn't have equality deletes
                        partitionSpec.fields().forEach(f -> partitionFields.put(f.fieldId(), new PartitionFieldInfo(partitionType.field(f.fieldId()), f)));
                        List<Integer> partitionFieldIds = partitionSpec.fields().stream().map(PartitionField::fieldId).collect(Collectors.toList());
                        HashSet<Integer> result = new HashSet<>();
                        result.addAll(delete.equalityFieldIds());
                        result.addAll(partitionFieldIds);
                        deleteInformations.put(ImmutableSet.copyOf(result), new DeleteSetInfo(partitionFields, delete.equalityFieldIds()));
                    }
                }
            }
            catch (IOException e) {
                return ImmutableMap.of();
            }
            return ImmutableMap.copyOf(deleteInformations);
        }

        private TableScanNode createDeletesTableScan(TableScanNode node,
                ImmutableMap<VariableReferenceExpression, ColumnHandle> deleteColumnAssignments,
                IcebergTableHandle icebergTableHandle,
                IcebergTableName tableName,
                List<Types.NestedField> deleteFields,
                TableHandle table,
                DeleteSetInfo deleteInfo)
        {
            List<VariableReferenceExpression> outputs = deleteColumnAssignments.keySet().asList();
            IcebergTableHandle deletesTableHandle = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                    new IcebergTableName(tableName.getTableName(),
                            TableType.EQUALITY_DELETES, // Read equality deletes instead of data
                            tableName.getSnapshotId(),
                            Optional.empty()),
                    icebergTableHandle.isSnapshotSpecified(),
                    icebergTableHandle.getPredicate(),
                    Optional.of(SchemaParser.toJson(new Schema(deleteFields))),
                    Optional.of(deleteInfo.equalityFieldIds)); // Enforce reading only delete files that match this schema

            return new TableScanNode(Optional.empty(),
                    idAllocator.getNextId(),
                    new TableHandle(table.getConnectorId(), deletesTableHandle, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                    outputs,
                    deleteColumnAssignments,
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
        }

        /**
         * - Updates table handle to DATA_WITHOUT_EQUALITY_DELETES since the page source for this node should now not apply equality deletes.
         * - Adds extra assignments and outputs that are needed by the join
         */
        private TableScanNode createNewRoot(TableScanNode node, IcebergTableHandle icebergTableHandle, IcebergTableName tableName, ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments, TableHandle table)
        {
            IcebergTableHandle updatedHandle = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                    new IcebergTableName(tableName.getTableName(),
                            TableType.DATE_WITHOUT_EQUALITY_DELETES, // Don't apply equality deletes in the split
                            tableName.getSnapshotId(),
                            tableName.getChangelogEndSnapshot()),
                    icebergTableHandle.isSnapshotSpecified(),
                    icebergTableHandle.getPredicate(),
                    icebergTableHandle.getTableSchemaJson(),
                    icebergTableHandle.getEqualityFieldIds());

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

        /**
         * Calculate required fields that the user didn't include in his select statement and add assignments for them to add to the table scan
         */
        private ImmutableMap<VariableReferenceExpression, ColumnHandle> createAssignmentsForUnselectedFields(TableScanNode node,
                ImmutableMap<Set<Integer>, DeleteSetInfo> deleteInfos,
                Table icebergTable)
        {
            Set<Integer> selectedFields = node.getAssignments().values().stream().map(f -> ((IcebergColumnHandle) f).getId()).collect(Collectors.toSet());
            Set<Integer> unselectedFields = Sets.difference(deleteInfos.keySet().stream().reduce(Sets::union).orElseGet(Collections::emptySet), selectedFields);
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> unselectedAssignmentsBuilder = ImmutableMap.builder();
            ImmutableMap<Integer, PartitionFieldInfo> partitionFields = deleteInfos.values().stream()
                    .map(DeleteSetInfo::getPartitionFields)
                    .reduce((a, b) -> new ImmutableMap.Builder<Integer, PartitionFieldInfo>().putAll(a).putAll(b).build())
                    .orElseGet(ImmutableMap::of);
            unselectedFields
                    .forEach(fieldId -> {
                        if (partitionFields.containsKey(fieldId)) {
                            PartitionFieldInfo partitionFieldInfo = partitionFields.get(fieldId);
                            PartitionField partitionField = partitionFieldInfo.getPartitionField();
                            Types.NestedField sourceField = icebergTable.schema().findField(partitionField.sourceId());
                            if (!partitionField.transform().isIdentity()) {
                                if (!partitionField.transform().isIdentity()) {
                                    Type partitionFieldType = partitionField.transform().getResultType(sourceField.type());
                                    VariableReferenceExpression variableReference = variableAllocator.newVariable(partitionField.name(), toPrestoType(partitionFieldType, typeManager));
                                    IcebergColumnHandle columnHandle = new IcebergColumnHandle(
                                            ColumnIdentity.createColumnIdentity(partitionField.name(), partitionField.fieldId(), partitionFieldType),
                                            toPrestoType(partitionFieldType, typeManager),
                                            Optional.empty(),
                                            PARTITION_KEY);
                                    unselectedAssignmentsBuilder.put(variableReference, columnHandle);
                                }
                            }
                            else if (!selectedFields.contains(sourceField.fieldId())) {
                                unselectedAssignmentsBuilder.put(
                                        variableAllocator.newVariable(sourceField.name(), toPrestoType(sourceField.type(), typeManager)),
                                        IcebergColumnHandle.create(sourceField, typeManager, REGULAR));
                            }
                        }
                        else {
                            Types.NestedField schemaField = icebergTable.schema().findField(fieldId);
                            unselectedAssignmentsBuilder.put(
                                    variableAllocator.newVariable(schemaField.name(), toPrestoType(schemaField.type(), typeManager)),
                                    IcebergColumnHandle.create(schemaField, typeManager, REGULAR));
                        }
                    });
            return unselectedAssignmentsBuilder.build();
        }

        private VariableReferenceExpression toVariableReference(IcebergColumnHandle c)
        {
            return variableAllocator.newVariable(c.getName(), c.getType());
        }

        private IcebergColumnHandle toIcebergColumnHandle(Types.NestedField field)
        {
            ColumnIdentity columnIdentity = new ColumnIdentity(field.fieldId(), field.name(), ColumnIdentity.TypeCategory.PRIMITIVE, Collections.emptyList());
            return new IcebergColumnHandle(columnIdentity, toPrestoType(field.type(), typeManager), Optional.empty(), REGULAR);
        }

        private VariableReferenceExpression toVariableReference(Types.NestedField field)
        {
            return variableAllocator.newVariable(field.name(), toPrestoType(field.type(), typeManager));
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
        }

        private static class DeleteSetInfo
        {
            private final ImmutableMap<Integer, PartitionFieldInfo> partitionFields;
            private final Set<Integer> equalityFieldIds;

            private DeleteSetInfo(Map<Integer, PartitionFieldInfo> partitionFields,
                    List<Integer> equalityFieldIds)
            {
                this.partitionFields = ImmutableMap.copyOf(partitionFields);
                this.equalityFieldIds = ImmutableSet.copyOf(equalityFieldIds);
            }

            public ImmutableMap<Integer, PartitionFieldInfo> getPartitionFields()
            {
                return partitionFields;
            }

            public List<Types.NestedField> allFields(Schema schema)
            {
                return Stream.concat(equalityFieldIds
                                .stream()
                                .map(schema::findField),
                        partitionFields
                                .values()
                                .stream()
                                .map(partitionFieldInfo -> {
                                    if (partitionFieldInfo.partitionField.transform().isIdentity()) {
                                        return schema.findField(partitionFieldInfo.partitionField.sourceId());
                                    }
                                    return partitionFieldInfo.nestedField;
                                })).collect(Collectors.toList());
            }
        }
    }
}
