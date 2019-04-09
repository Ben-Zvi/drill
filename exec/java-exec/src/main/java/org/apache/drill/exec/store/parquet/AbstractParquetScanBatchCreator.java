/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.expr.FilterPredicate;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.stat.RowsMatch;
import org.apache.drill.exec.physical.base.AbstractGroupScanWithMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.store.parquet.metadata.Metadata_V3;
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.shaded.guava.com.google.common.base.Functions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.store.parquet2.DrillParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class AbstractParquetScanBatchCreator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetScanBatchCreator.class);

  protected ScanBatch getBatch(ExecutorFragmentContext context, AbstractParquetRowGroupScan rowGroupScan,
                               OperatorContext oContext) throws ExecutionSetupException {
    final ColumnExplorer columnExplorer = new ColumnExplorer(context.getOptions(), rowGroupScan.getColumns());

    if (!columnExplorer.isStarQuery()) {
      rowGroupScan = rowGroupScan.copy(columnExplorer.getTableColumns());
      rowGroupScan.setOperatorId(rowGroupScan.getOperatorId());
    }

    AbstractDrillFileSystemManager fsManager = getDrillFileSystemCreator(oContext, context.getOptions());

    // keep footers in a map to avoid re-reading them
    Map<Path, ParquetMetadata> footers = new HashMap<>();
    List<RecordReader> readers = new LinkedList<>();
    List<Map<String, String>> implicitColumns = new ArrayList<>();
    Map<String, String> mapWithMaxColumns = new LinkedHashMap<>();
    ParquetReaderConfig readerConfig = rowGroupScan.getReaderConfig();
    RowGroupReadEntry firstRowGroup = null; // to be scanned in case ALL row groups are pruned out
    ParquetMetadata firstFooter = null;
    long rowgroupsPruned = 0; // for stats
    TupleSchema tupleSchema = rowGroupScan.getTupleSchema();

    try {

      LogicalExpression filterExpr = rowGroupScan.getFilter();
      boolean doRuntimePruning = filterExpr != null && // was a filter given ?   And it is not just a "TRUE" predicate
        ! ((filterExpr instanceof ValueExpressions.BooleanExpression) && ((ValueExpressions.BooleanExpression) filterExpr).getBoolean() );
      Path selectionRoot = rowGroupScan.getSelectionRoot();
      // Runtime pruning: Avoid recomputing metadata objects for each row-group in case they use the same file
      // by keeping the following objects computed earlier (relies on same file being in consecutive rowgroups)
      Path prevRowGroupPath = null;
      Metadata_V3.ParquetTableMetadata_v3 tableMetadataV3 = null;
      Metadata_V3.ParquetFileMetadata_v3 fileMetadataV3 = null;
      FileSelection fileSelection = null;
      FilterPredicate filterPredicate = null;
      Set<SchemaPath> schemaPathsInExpr = null;
      Set<String> columnsInExpr = null;

      // If pruning - Prepare the predicate and the columns before the FOR LOOP
      if ( doRuntimePruning ) {
        filterPredicate = AbstractGroupScanWithMetadata.getFilterPredicate(filterExpr, context,
          (FunctionImplementationRegistry) context.getFunctionRegistry(), context.getOptions(), true,
          true /* supports file implicit columns */,
          tupleSchema);
        schemaPathsInExpr = filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);
        columnsInExpr = new HashSet<>();
        for (SchemaPath path : schemaPathsInExpr) {
          columnsInExpr.add(path.getRootSegmentPath());
        }
      }

      for (RowGroupReadEntry rowGroup : rowGroupScan.getRowGroupReadEntries()) {
        /*
        Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
        TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
        we should add more information to the RowGroupInfo that will be populated upon the first read to
        provide the reader with all of th file meta-data it needs
        These fields will be added to the constructor below
        */

          Stopwatch timer = logger.isTraceEnabled() ? Stopwatch.createUnstarted() : null;
          DrillFileSystem fs = fsManager.get(rowGroupScan.getFsConf(rowGroup), rowGroup.getPath());
          if (!footers.containsKey(rowGroup.getPath())) {
            if (timer != null) {
              timer.start();
            }

            ParquetMetadata footer = readFooter(fs.getConf(), rowGroup.getPath(), readerConfig);
            if (timer != null) {
              long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
              logger.trace("ParquetTrace,Read Footer,{},{},{},{},{},{},{}", "", rowGroup.getPath(), "", 0, 0, 0, timeToRead);
            }
            footers.put(rowGroup.getPath(), footer);
          }
          ParquetMetadata footer = footers.get(rowGroup.getPath());

          //
          //   If a filter is given (and it is not just "TRUE") - then use it to perform run-time pruning
          //
          if ( doRuntimePruning  ) { // skip when no filter or filter is TRUE

            int rowGroupIndex = rowGroup.getRowGroupIndex();
            long footerRowCount = footer.getBlocks().get(rowGroupIndex).getRowCount();

            if ( timer != null ) {  // restart the timer, if tracing
              timer.reset();
              timer.start();
            }

            // When starting a new file, or at the first time - Initialize the path specific metadata
            if ( ! rowGroup.getPath().equals(prevRowGroupPath) ) {
              // Create a table metadata (V3)
              tableMetadataV3 = new Metadata_V3.ParquetTableMetadata_v3();

              // The file status for this file
              FileStatus fileStatus = fs.getFileStatus(rowGroup.getPath());

              // The file metadata (only for the columns used in the filter)
              fileMetadataV3 = Metadata.getParquetFileMetadata_v3(tableMetadataV3, footer, fileStatus, fs, false, columnsInExpr, readerConfig);

              prevRowGroupPath = rowGroup.getPath(); // for next time
            }

            MetadataBase.RowGroupMetadata rowGroupMetadata = fileMetadataV3.getRowGroups().get(rowGroup.getRowGroupIndex());

            Map<SchemaPath, ColumnStatistics> columnsStatistics = ParquetTableMetadataUtils.getRowGroupColumnStatistics(tableMetadataV3, rowGroupMetadata);

            //
            // Perform the Run-Time Pruning - i.e. Skip this rowgroup if the match fails
            //
            RowsMatch match = FilterEvaluatorUtils.matches(filterPredicate, columnsStatistics, footerRowCount);
            if (timer != null) { // if tracing
              long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
              logger.trace("Run-time pruning: {} row-group {} (RG index: {} row count: {}), took {} usec", match == RowsMatch.NONE ? "Excluded" : "Included", rowGroup.getPath(),
                rowGroupIndex, footerRowCount, timeToRead);
            }
            if (match == RowsMatch.NONE) {
              rowgroupsPruned++; // one more RG was pruned
              if (firstRowGroup == null) {  // keep first RG, to be used in case all row groups are pruned
                firstRowGroup = rowGroup;
                firstFooter = footer;
              }
              continue; // This Row group does not comply with the filter - prune it out and check the next Row Group
            }
          }

          mapWithMaxColumns = createReaderAndImplicitColumns(context, rowGroupScan, oContext, columnExplorer, readers, implicitColumns, mapWithMaxColumns, rowGroup,
           fs, footer /*, false */);
      }

      // in case all row groups were pruned out - create a single reader for the first one (so that the schema could be returned)
      if ( readers.size() == 0 && firstRowGroup != null ) {
        logger.trace("All row groups were pruned out. Returning the first: {} (row count {}) for its schema", firstRowGroup.getPath(), firstRowGroup.getNumRecordsToRead());
        DrillFileSystem fs = fsManager.get(rowGroupScan.getFsConf(firstRowGroup), firstRowGroup.getPath());
        mapWithMaxColumns = createReaderAndImplicitColumns(context, rowGroupScan, oContext, columnExplorer, readers, implicitColumns, mapWithMaxColumns, firstRowGroup, fs,
          firstFooter /*, true */);
      }

      // Update stats (same in every reader - the others would just overwrite the stats)
      for (RecordReader rr : readers ) {
        if ( rr instanceof ParquetRecordReader ) {
          ((ParquetRecordReader) rr).updateRowgroupsStats(rowGroupScan.getRowGroupReadEntries().size(), rowgroupsPruned);
        }
      }

    } catch (IOException|InterruptedException e) {
      throw new ExecutionSetupException(e);
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant(null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(context, oContext, readers, implicitColumns);
  }

  /**
   *  Create a reader and add it to the list of readers.
   *
   * @param context The fragment context
   * @param rowGroupScan RowGroup Scan
   * @param oContext Operator context
   * @param columnExplorer The column helper class object
   * @param readers the readers' list where a new reader is added to
   * @param implicitColumns the implicit columns list
   * @param mapWithMaxColumns To be modified, in case there are implicit columns
   * @param rowGroup create a reader for this specific row group
   * @param fs file system
   * @param footer this file's footer
   * // @param readSchemaOnly - if true sets the number of rows to read to be zero
   * @return the (possibly modified) input  mapWithMaxColumns
   */
  private Map<String, String> createReaderAndImplicitColumns(ExecutorFragmentContext context,
                                                             AbstractParquetRowGroupScan rowGroupScan,
                                                             OperatorContext oContext,
                                                             ColumnExplorer columnExplorer,
                                                             List<RecordReader> readers,
                                                             List<Map<String, String>> implicitColumns,
                                                             Map<String, String> mapWithMaxColumns,
                                                             RowGroupReadEntry rowGroup,
                                                             DrillFileSystem fs,
                                                             ParquetMetadata footer
                                                             // ,boolean readSchemaOnly -- TODO:
  ) {
    ParquetReaderConfig readerConfig = rowGroupScan.getReaderConfig();
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(footer,
      rowGroupScan.getColumns(), readerConfig.autoCorrectCorruptedDates());
    logger.debug("Contains corrupt dates: {}.", containsCorruptDates);

    boolean useNewReader = context.getOptions().getBoolean(ExecConstants.PARQUET_NEW_RECORD_READER);
    boolean containsComplexColumn = ParquetReaderUtility.containsComplexColumn(footer, rowGroupScan.getColumns());
    logger.debug("PARQUET_NEW_RECORD_READER is {}. Complex columns {}.", useNewReader ? "enabled" : "disabled",
        containsComplexColumn ? "found." : "not found.");
    RecordReader reader;

    if (useNewReader || containsComplexColumn) {
      reader = new DrillParquetReader(context,
          footer,
          rowGroup,
          columnExplorer.getTableColumns(),
          fs,
          containsCorruptDates);
    } else {
      reader = new ParquetRecordReader(context,
          rowGroup.getPath(),
          rowGroup.getRowGroupIndex(),
          rowGroup.getNumRecordsToRead(), // TODO: if readSchemaOnly - then set to zero rows to read (currently breaks the ScanBatch)
          fs,
          CodecFactory.createDirectCodecFactory(fs.getConf(), new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
          footer,
          rowGroupScan.getColumns(),
          containsCorruptDates);
    }

    logger.debug("Query {} uses {}",
        QueryIdHelper.getQueryId(oContext.getFragmentContext().getHandle().getQueryId()),
        reader.getClass().getSimpleName());
    readers.add(reader);

    List<String> partitionValues = rowGroupScan.getPartitionValues(rowGroup);
    Map<String, String> implicitValues = columnExplorer.populateImplicitColumns(rowGroup.getPath(), partitionValues, rowGroupScan.supportsFileImplicitColumns());
    implicitColumns.add(implicitValues);
    if (implicitValues.size() > mapWithMaxColumns.size()) {
      mapWithMaxColumns = implicitValues;
    }
    return mapWithMaxColumns;
  }

  protected abstract AbstractDrillFileSystemManager getDrillFileSystemCreator(OperatorContext operatorContext, OptionManager optionManager);

  private ParquetMetadata readFooter(Configuration conf, Path path, ParquetReaderConfig readerConfig) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path,
      readerConfig.addCountersToConf(conf)), readerConfig.toReadOptions())) {
      return reader.getFooter();
    }
  }

  /**
   * Helper class responsible for creating and managing DrillFileSystem.
   */
  protected abstract class AbstractDrillFileSystemManager {

    protected final OperatorContext operatorContext;

    protected AbstractDrillFileSystemManager(OperatorContext operatorContext) {
      this.operatorContext = operatorContext;
    }

    protected abstract DrillFileSystem get(Configuration config, Path path) throws ExecutionSetupException;
  }
}
