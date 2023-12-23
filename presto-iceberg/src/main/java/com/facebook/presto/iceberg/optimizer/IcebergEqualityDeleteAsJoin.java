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
import com.facebook.presto.spi.relation.RowExpressionService;
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
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;

    IcebergEqualityDeleteAsJoin(StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            IcebergTransactionManager transactionManager,
            TypeManager typeManager)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isDeleteToJoinPushdownEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new DeleteAsJoinRewriter(functionResolution, rowExpressionService,
                transactionManager, idAllocator, session, typeManager, variableAllocator), maxSubplan);
    }

    private static class DeleteAsJoinRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final RowExpressionService rowExpressionService;
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager transactionManager;
        private final TypeManager typeManager;
        private final VariableAllocator variableAllocator;

        public DeleteAsJoinRewriter(
                StandardFunctionResolution functionResolution,
                RowExpressionService rowExpressionService,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session,
                TypeManager typeManager,
                VariableAllocator variableAllocator)
        {
            this.functionResolution = functionResolution;
            this.rowExpressionService = rowExpressionService;
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
            if (!icebergTableHandle.getTableName().getSnapshotId().isPresent() ||
                    icebergTableHandle.getTableName().getTableType() != TableType.DATA) {
                return node;
            }
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(table.getTransaction());
            Table icebergTable = getIcebergTable(metadata, session, icebergTableHandle.getSchemaTableName());
            HashSet<Set<Integer>> deleteSchemas = new HashSet<>();
            Map<Integer, PartitionField> partitionFields = new HashMap<>();
            try (CloseableIterator<DeleteFile> files =
                    IcebergUtil.getDeleteFiles(icebergTable, icebergTableHandle.getTableName().getSnapshotId().get(), icebergTableHandle.getPredicate()).iterator()) {
                while (files.hasNext()) {
                    DeleteFile delete = files.next();
                    if (delete.content() == FileContent.EQUALITY_DELETES) {
                        PartitionSpec partitionSpec = icebergTable.specs().get(delete.specId());
                        partitionSpec.fields().forEach(f -> partitionFields.put(f.fieldId(), f));
                        List<Integer> partitionFieldIds = partitionSpec.fields().stream().map(PartitionField::sourceId).collect(Collectors.toList());
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
                return node;
            }

            // Add all the fields required by the join that were not added by the users query
            Set<Integer> selectedFields = node.getAssignments().values().stream().map(f -> ((IcebergColumnHandle) f).getId()).collect(Collectors.toSet());
            Set<Integer> unselectedFields = Sets.difference(deleteSchemas.stream().reduce(Sets::union).get(), selectedFields);
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> unselectedAssignmentsBuilder = ImmutableMap.builder();
            unselectedFields
                    .forEach(fieldId -> {
                        if (partitionFields.containsKey(fieldId)) {
                            // TODO: Is this code path needed? (Maybe it is needed with bucketed partitions?)
                            PartitionField partitionField = partitionFields.get(fieldId);
                            Types.NestedField sourceField = icebergTable.schema().findField(partitionField.sourceId());
                            Type partitionFieldType = TypeConverter.toPrestoType(partitionField.transform().getResultType(sourceField.type()), typeManager);
                            unselectedAssignmentsBuilder.put(
                                    variableAllocator.newVariable(partitionField.name(), partitionFieldType),
                                    IcebergColumnHandle.create(sourceField, typeManager, PARTITION_KEY));
                        }
                        else {
                            Types.NestedField schemaField = icebergTable.schema().findField(fieldId);
                            unselectedAssignmentsBuilder.put(
                                    variableAllocator.newVariable(schemaField.name(), TypeConverter.toPrestoType(schemaField.type(), typeManager)),
                                    IcebergColumnHandle.create(schemaField, typeManager, REGULAR));
                        }
                    });
            ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments = unselectedAssignmentsBuilder.build();

            IcebergTableName oldTableName = icebergTableHandle.getTableName();
            IcebergTableName updatedTableName =
                    new IcebergTableName(oldTableName.getTableName(), TableType.DATE_WITHOUT_EQUALITY_DELETES, oldTableName.getSnapshotId(), oldTableName.getChangelogEndSnapshot());
            IcebergTableHandle updatedHandle = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                    updatedTableName,
                    icebergTableHandle.isSnapshotSpecified(),
                    icebergTableHandle.getPredicate(),
                    icebergTableHandle.getTableSchemaJson());
            VariableReferenceExpression dataSequenceNumberVariableReference = toVariableReference(IcebergColumnHandle.dataSequenceNumberColumnHandle());
            ImmutableMap<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                    .put(dataSequenceNumberVariableReference, IcebergColumnHandle.dataSequenceNumberColumnHandle())
                    .putAll(unselectedAssignments)
                    .putAll(node.getAssignments())
                    .build();
            ImmutableList.Builder<VariableReferenceExpression> outputsBuilder = ImmutableList.builder();
            outputsBuilder.addAll(node.getOutputVariables());
            if (!node.getAssignments().containsKey(dataSequenceNumberVariableReference)) {
                outputsBuilder.add(dataSequenceNumberVariableReference);
            }
            outputsBuilder.addAll(ImmutableList.copyOf(unselectedAssignments.keySet()));

            PlanNode parentJoin = new TableScanNode(node.getSourceLocation(),
                    node.getId(),
                    new TableHandle(table.getConnectorId(), updatedHandle, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                    outputsBuilder.build(),
                    assignments,
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
            List<RowExpression> deleteVersionColumns = new ArrayList<>();

            Map<Integer, VariableReferenceExpression> reverseAssignmentsMap =
                    assignments.entrySet().stream().collect(Collectors.toMap(x -> ((IcebergColumnHandle) (x.getValue())).getId(), Map.Entry::getKey));
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
                VariableReferenceExpression sourceSequenceNumber = findMatchingVariable(parentJoin.getOutputVariables(), DATA_SEQUENCE_NUMBER.getColumnName());
                FunctionHandle lessThan = functionResolution.comparisonFunction(OperatorType.LESS_THAN, BigintType.BIGINT, BigintType.BIGINT);
                RowExpression versionFilter = new CallExpression(lessThan.getName(),
                        lessThan,
                        BooleanType.BOOLEAN,
                        Collections.unmodifiableList(Arrays.asList(sourceSequenceNumber, joinSequenceNumber)));

                List<VariableReferenceExpression> outputs = deleteColumnAssignments.keySet().asList();
                IcebergTableName icebergTableName = icebergTableHandle.getTableName();
                IcebergTableHandle deletesTableHanlde = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                        new IcebergTableName(icebergTableName.getTableName(), TableType.EQUALITY_DELETES, icebergTableName.getSnapshotId(), Optional.empty()),
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

        private ColumnIdentity toColumnIdentity(Types.NestedField nestedField)
        {
            return new ColumnIdentity(nestedField.fieldId(), nestedField.name(), ColumnIdentity.TypeCategory.PRIMITIVE, Collections.emptyList());
        }

        private VariableReferenceExpression findMatchingVariable(List<VariableReferenceExpression> columns, String nameToMatch)
        {
            return columns.stream().filter(f -> f.getName().startsWith(nameToMatch)).findFirst().get();
        }
    }
}
