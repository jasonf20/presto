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
package com.facebook.presto.iceberg.equalitydeletes;

import com.facebook.presto.iceberg.IcebergSplit;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.iceberg.IcebergSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.Iterators.limit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class EqualityDeletesSplitSource
        implements ConnectorSplitSource
{
    private final ConnectorSession session;
    private final Map<Integer, PartitionSpec> specById;
    private final CloseableIterator<DeleteFile> deleteFiles;

    public EqualityDeletesSplitSource(
            ConnectorSession session,
            Table table,
            CloseableIterable<DeleteFile> deleteFiles)
    {
        this.session = requireNonNull(session, "session is null");
        this.specById = table.specs();
        requireNonNull(deleteFiles, "deleteFiles is null");
        this.deleteFiles = CloseableIterable.filter(deleteFiles, f -> f.content() == FileContent.EQUALITY_DELETES).iterator();
    }

    @Override
    public boolean isFinished()
    {
        return !deleteFiles.hasNext();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<DeleteFile> iterator = limit(deleteFiles, maxSize);
        while (iterator.hasNext()) {
            DeleteFile manifestReader = iterator.next();
            splits.add(toIcebergSplit(manifestReader));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public void close()
    {
        try {
            deleteFiles.close();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private ConnectorSplit toIcebergSplit(DeleteFile manifesReader)
    {
        return splitFromDeleteFile(manifesReader);
    }

    private IcebergSplit splitFromDeleteFile(DeleteFile deleteFile)
    {
        return new IcebergSplit(
                deleteFile.path().toString(),
                0,
                deleteFile.fileSizeInBytes(),
                deleteFile.format(),
                ImmutableList.of(),
                getPartitionKeys(specById.get(deleteFile.specId()), deleteFile.partition()),
                getNodeSelectionStrategy(session),
                SplitWeight.standard(),
                ImmutableList.of(),
                Optional.empty(),
                IcebergUtil.getDataSequenceNumber(deleteFile));
    }
}
