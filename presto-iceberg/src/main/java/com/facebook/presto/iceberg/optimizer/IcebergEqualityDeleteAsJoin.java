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
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.TypeConverter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.CanonicalJoinNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.Sets;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
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
        return rewriteWith(new DeleteAsJoinRewriter(functionResolution, rowExpressionService,
                transactionManager, idAllocator, session, typeManager), maxSubplan);
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

        private int fieldSuffix;

        public DeleteAsJoinRewriter(
                StandardFunctionResolution functionResolution,
                RowExpressionService rowExpressionService,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session,
                TypeManager typeManager)
        {
            this.functionResolution = functionResolution;
            this.rowExpressionService = rowExpressionService;
            this.transactionManager = transactionManager;
            this.idAllocator = idAllocator;
            this.session = session;
            this.typeManager = typeManager;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle table = node.getTable();
            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table.getConnectorHandle();
            if (!icebergTableHandle.getTableName().getSnapshotId().isPresent()) {
                // Split manager returns no splits in this case
                return node;
            }
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(table.getTransaction());
            Table icebergTable = getIcebergTable(metadata, session, icebergTableHandle.getSchemaTableName());
            TableScan tableScan = icebergTable.newScan()
                    .filter(toIcebergExpression(icebergTableHandle.getPredicate()))
                    .useSnapshot(icebergTableHandle.getTableName().getSnapshotId().get());
            HashSet<Set<Integer>> deleteSchemas = new HashSet<>();
            try (CloseableIterable<FileScanTask> files = tableScan.planFiles()) {
                try (CloseableIterator<FileScanTask> iterator = files.iterator()) {
//                    HashMap<Types.StructType, HashSet<String>> deleteSchemas = new HashMap<>();
                    while (iterator.hasNext()) {
                        FileScanTask task = iterator.next();
                        for (DeleteFile delete : task.deletes()) {
                            deleteSchemas.add(Sets.newHashSet(delete.equalityFieldIds()));
                        }
                    }
                }
            }
            catch (IOException e) {
                return node;
            }
//            if (deleteSchemas.isEmpty()) {
//                return node;
//            }
            PlanNode parentJoin = node;
            List<RowExpression> deleteVersionColumns = new ArrayList<>();

            for (Set<Integer> deleteSchema : deleteSchemas) {
                List<Types.NestedField> deleteFields = deleteSchema
                        .stream()
                        .map(fieldId -> icebergTable.schema().findField(fieldId))
                        .collect(Collectors.toList());
                List<IcebergColumnHandle> deleteColumnHandles = deleteFields.stream().map(this::toIcebergColumnHandle).collect(Collectors.toList());
                Set<CanonicalJoinNode.EquiJoinClause> clauses = deleteColumnHandles.stream()
                        .map(column -> {
                            VariableReferenceExpression left = toVariableReference(column);
                            VariableReferenceExpression right = toVariableReference(column);
                            return new CanonicalJoinNode.EquiJoinClause(left, right);
                        }).collect(Collectors.toSet());
                IcebergTableHandle deletesTableHanlde = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                        icebergTableHandle.getTableName(),
                        icebergTableHandle.isSnapshotSpecified(),
                        icebergTableHandle.getPredicate(),
                        Optional.of(new Schema(deleteFields).toString()));
                VariableReferenceExpression sourceSequenceNumber = new VariableReferenceExpression(Optional.empty(), getFieldName("$data_sequence_number"), BigintType.BIGINT);
                VariableReferenceExpression joinSequenceNumber = new VariableReferenceExpression(Optional.empty(), getFieldName("$data_sequence_number"), BigintType.BIGINT);
                FunctionHandle lessThan = functionResolution.comparisonFunction(OperatorType.LESS_THAN, BigintType.BIGINT, BigintType.BIGINT);
                RowExpression versionFilter = new CallExpression(lessThan.getName(),
                        lessThan,
                        BooleanType.BOOLEAN,
                        Collections.unmodifiableList(Arrays.asList(sourceSequenceNumber, joinSequenceNumber)));

                List<VariableReferenceExpression> outputs = deleteColumnHandles.stream().map(DeleteAsJoinRewriter::toVariableReference).collect(Collectors.toList());
                outputs.add(joinSequenceNumber);
                Map<VariableReferenceExpression, ColumnHandle> assignments =
                        deleteColumnHandles.stream().collect(Collectors.toMap(DeleteAsJoinRewriter::toVariableReference, c -> c));
                assignments.put(joinSequenceNumber, toSyntheticColumnHandle(joinSequenceNumber));
                TableScanNode deleteTableScan = new TableScanNode(Optional.empty(),
                        idAllocator.getNextId(),
                        new TableHandle(table.getConnectorId(), deletesTableHanlde, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                        outputs,
                        assignments,
                        node.getCurrentConstraint(),
                        node.getEnforcedConstraint());

                parentJoin = new CanonicalJoinNode(idAllocator.getNextId(), Arrays.asList(parentJoin, deleteTableScan),
                        CanonicalJoinNode.Type.LEFT,
                        clauses,
                        Sets.newHashSet(versionFilter),
                        node.getOutputVariables());
            }

            FilterNode filter = new FilterNode(Optional.empty(), idAllocator.getNextId(), Optional.empty(), parentJoin,
                    new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL, BooleanType.BOOLEAN,
                            new SpecialFormExpression(SpecialFormExpression.Form.COALESCE, BigintType.BIGINT, deleteVersionColumns)));

            return node;
        }

        private static IcebergColumnHandle toSyntheticColumnHandle(VariableReferenceExpression joinSequenceNumber)
        {
            return new IcebergColumnHandle(new ColumnIdentity(10002, joinSequenceNumber.getName(), ColumnIdentity.TypeCategory.PRIMITIVE, Collections.emptyList()), joinSequenceNumber.getType(), Optional.empty(), IcebergColumnHandle.ColumnType.METADATA);
        }

        private static VariableReferenceExpression toVariableReference(IcebergColumnHandle c)
        {
            return new VariableReferenceExpression(Optional.empty(), c.getName(), c.getType());
        }

        private String getFieldName(String baseName)
        {
            return baseName + "_" + fieldSuffix++;
        }

        private IcebergColumnHandle toIcebergColumnHandle(Types.NestedField f)
        {
            return new IcebergColumnHandle(toColumnIdentity(f), TypeConverter.toPrestoType(f.type(), typeManager), Optional.empty(), IcebergColumnHandle.ColumnType.REGULAR);
        }

        private VariableReferenceExpression toVariableReference(Types.NestedField field)
        {
            return new VariableReferenceExpression(Optional.empty(), field.name(), TypeConverter.toPrestoType(field.type(), typeManager));
        }

        private ColumnIdentity toColumnIdentity(Types.NestedField nestedField)
        {
            return new ColumnIdentity(nestedField.fieldId(), getFieldName(nestedField.name()), ColumnIdentity.TypeCategory.PRIMITIVE, Collections.emptyList());
        }
    }
}
