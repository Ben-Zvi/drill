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

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.FilterBuilder;
import org.apache.drill.exec.expr.FilterPredicate;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.stat.RowsMatch;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.store.parquet.metadata.Metadata_V3;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.shaded.guava.com.google.common.base.Functions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbstractParquetScanBatchCreator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetScanBatchCreator.class);

/*
 *
 *
 *    The code below is copied and adapted
 *
 *
 */

  private class ColTypeInfo {
    public OriginalType originalType;
    public int precision;
    public int scale;
    public int repetitionLevel;
    public int definitionLevel;

    ColTypeInfo(OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel) {
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
    }
  }

  /**
   * Get the host affinity for a row group.
   *
   * @param fileStatus the parquet file
   * @param start      the start of the row group
   * @param length     the length of the row group
   * @return host affinity for the row group
   */
  private Map<String, Float> getHostAffinity(FileStatus fileStatus, FileSystem fs, long start, long length)
    throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String, Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) -
          (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }


  private ColTypeInfo getColTypeInfo(MessageType schema, Type type, String[] path, int depth) {
    if (type.isPrimitive()) {
      PrimitiveType primitiveType = (PrimitiveType) type;
      int precision = 0;
      int scale = 0;
      if (primitiveType.getDecimalMetadata() != null) {
        precision = primitiveType.getDecimalMetadata().getPrecision();
        scale = primitiveType.getDecimalMetadata().getScale();
      }

      int repetitionLevel = schema.getMaxRepetitionLevel(path);
      int definitionLevel = schema.getMaxDefinitionLevel(path);

      return new ColTypeInfo(type.getOriginalType(), precision, scale, repetitionLevel, definitionLevel);
    }
    Type t = ((GroupType) type).getType(path[depth]);
    return getColTypeInfo(schema, t, path, depth + 1);
  }


  /**
   * Get the metadata for a single file
   */
  private Metadata_V3.ParquetFileMetadata_v3 getParquetFileMetadata_v3(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata,
                                                                      final FileStatus file, final FileSystem fs, boolean allColumns, Set<String> columnSet, ParquetReaderConfig readerConfig) throws IOException,
    InterruptedException {
    final ParquetMetadata metadata;
    final UserGroupInformation processUserUgi = ImpersonationUtil.getProcessUserUGI();
    final Configuration conf = new Configuration(fs.getConf());
    try {
      metadata = processUserUgi.doAs((PrivilegedExceptionAction<ParquetMetadata>)() -> {
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromStatus(file, conf), readerConfig.toReadOptions())) {
          return parquetFileReader.getFooter();
        }
      });
    } catch(Exception e) {
      logger.error("Exception while reading footer of parquet file [Details - path: {}, owner: {}] as process user {}",
        file.getPath(), file.getOwner(), processUserUgi.getShortUserName(), e);
      throw e;
    }

    MessageType schema = metadata.getFileMetaData().getSchema();

    Map<SchemaPath, ColTypeInfo> colTypeInfoMap = new HashMap<>();
    schema.getPaths();
    for (String[] path : schema.getPaths()) {
      colTypeInfoMap.put(SchemaPath.getCompoundPath(path), getColTypeInfo(schema, schema, path, 0));
    }

    List<Metadata_V3.RowGroupMetadata_v3> rowGroupMetadataList = Lists.newArrayList();

    ArrayList<SchemaPath> ALL_COLS = new ArrayList<>();
    ALL_COLS.add(SchemaPath.STAR_COLUMN);
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(metadata, ALL_COLS,
      readerConfig.autoCorrectCorruptedDates());
    logger.debug("Contains corrupt dates: {}.", containsCorruptDates);

    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<Metadata_V3.ColumnMetadata_v3> columnMetadataList = new ArrayList<>();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        String[] columnName = col.getPath().toArray();
        SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
        ColTypeInfo colTypeInfo = colTypeInfoMap.get(columnSchemaName);

        Metadata_V3.ColumnTypeMetadata_v3 columnTypeMetadata =
          new Metadata_V3.ColumnTypeMetadata_v3(columnName, col.getPrimitiveType().getPrimitiveTypeName(), colTypeInfo.originalType,
            colTypeInfo.precision, colTypeInfo.scale, colTypeInfo.repetitionLevel, colTypeInfo.definitionLevel);

        if (parquetTableMetadata.columnTypeInfo == null) {
          parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
        }
        parquetTableMetadata.columnTypeInfo.put(new Metadata_V3.ColumnTypeMetadata_v3.Key(columnTypeMetadata.name), columnTypeMetadata);
        // Store column metadata for all columns
          Statistics<?> stats = col.getStatistics();
          // Save the column schema info. We'll merge it into one list
          Object minValue = null;
          Object maxValue = null;
          long numNulls = -1;
          boolean statsAvailable = stats != null && !stats.isEmpty();
          if (statsAvailable) {
            if (stats.hasNonNullValue()) {
              minValue = stats.genericGetMin();
              maxValue = stats.genericGetMax();
              if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION && columnTypeMetadata.originalType == OriginalType.DATE) {
                minValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) minValue);
                maxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) maxValue);
              }
            }
            numNulls = stats.getNumNulls();
          }
          Metadata_V3.ColumnMetadata_v3 columnMetadata = new Metadata_V3.ColumnMetadata_v3(columnTypeMetadata.name, col.getPrimitiveType().getPrimitiveTypeName(), minValue, maxValue, numNulls);
          columnMetadataList.add(columnMetadata);

        length += col.getTotalSize();
      }

      // DRILL-5009: Skip the RowGroup if it is empty
      // Note we still read the schema even if there are no values in the RowGroup
      if (rowGroup.getRowCount() == 0) {
        continue;
      }
      Metadata_V3.RowGroupMetadata_v3 rowGroupMeta =
        new Metadata_V3.RowGroupMetadata_v3(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
          getHostAffinity(file, fs, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }
    Path path = Path.getPathWithoutSchemeAndAuthority(file.getPath());

    return new Metadata_V3.ParquetFileMetadata_v3(path, file.getLen(), rowGroupMetadataList);
  }

  private Metadata_V3.RowGroupMetadata_v3 getRowGroupMetadata(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata,
                                                              final FileStatus file,
                                                              final int rowGroupIndex,
                                                              final FileSystem fs,
                                                              ParquetReaderConfig readerConfig) throws IOException, InterruptedException {
    final ParquetMetadata metadata;
    final UserGroupInformation processUserUgi = ImpersonationUtil.getProcessUserUGI();
    final Configuration conf = new Configuration(fs.getConf());
    try {
      metadata = processUserUgi.doAs((PrivilegedExceptionAction<ParquetMetadata>)() -> {
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromStatus(file, conf), readerConfig.toReadOptions())) {
          return parquetFileReader.getFooter();
        }
      });
    } catch(Exception e) {
      logger.error("Exception while reading footer of parquet file [Details - path: {}, owner: {}] as process user {}",
        file.getPath(), file.getOwner(), processUserUgi.getShortUserName(), e);
      throw e;
    }

    MessageType schema = metadata.getFileMetaData().getSchema();

    Map<SchemaPath, ColTypeInfo> colTypeInfoMap = new HashMap<>();
    for (String[] path : schema.getPaths()) {
      colTypeInfoMap.put(SchemaPath.getCompoundPath(path), getColTypeInfo(schema, schema, path, 0));
    }

    ArrayList<SchemaPath> ALL_COLS = new ArrayList<>();
    ALL_COLS.add(SchemaPath.STAR_COLUMN);
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(metadata, ALL_COLS,
      readerConfig.autoCorrectCorruptedDates());
    logger.debug("Contains corrupt dates: {}.", containsCorruptDates);

    BlockMetaData rowGroup = metadata.getBlocks().get(rowGroupIndex);

    List<Metadata_V3.ColumnMetadata_v3> columnMetadataList = new ArrayList<>();
    long length = 0;
    for (ColumnChunkMetaData col : rowGroup.getColumns()) {
      String[] columnName = col.getPath().toArray();
      SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
      ColTypeInfo colTypeInfo = colTypeInfoMap.get(columnSchemaName);

      Metadata_V3.ColumnTypeMetadata_v3 columnTypeMetadata =
        new Metadata_V3.ColumnTypeMetadata_v3(columnName, col.getPrimitiveType().getPrimitiveTypeName(), colTypeInfo.originalType,
          colTypeInfo.precision, colTypeInfo.scale, colTypeInfo.repetitionLevel, colTypeInfo.definitionLevel);

      if (parquetTableMetadata.columnTypeInfo == null) {
        parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
      }
      parquetTableMetadata.columnTypeInfo.put(new Metadata_V3.ColumnTypeMetadata_v3.Key(columnTypeMetadata.name), columnTypeMetadata);
      Statistics<?> stats = col.getStatistics();
      // Save the column schema info. We'll merge it into one list
      Object minValue = null;
      Object maxValue = null;
      long numNulls = -1;
      boolean statsAvailable = stats != null && !stats.isEmpty();
      if (statsAvailable) {
        if (stats.hasNonNullValue()) {
          minValue = stats.genericGetMin();
          maxValue = stats.genericGetMax();
          if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION && columnTypeMetadata.originalType == OriginalType.DATE) {
            minValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) minValue);
            maxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) maxValue);
          }
        }
        numNulls = stats.getNumNulls();
      }
      Metadata_V3.ColumnMetadata_v3 columnMetadata = new Metadata_V3.ColumnMetadata_v3(columnTypeMetadata.name, col.getPrimitiveType().getPrimitiveTypeName(), minValue, maxValue, numNulls);
      columnMetadataList.add(columnMetadata);

      length += col.getTotalSize();
    }

    // DRILL-5009: Skip the RowGroup if it is empty
    // Note we still read the schema even if there are no values in the RowGroup
    //if (rowGroup.getRowCount() == 0) {
    //  return null;
    //}

    return new Metadata_V3.RowGroupMetadata_v3(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
      getHostAffinity(file, fs, rowGroup.getStartingPos(), length), columnMetadataList);
  }

  /**
   * Returns parquet filter predicate built from specified {@code filterExpr}.
   *
   * @param filterExpr                     filter expression to build
   * @param udfUtilities                   udf utilities
   * @param functionImplementationRegistry context to find drill function holder
   * @param omitUnsupportedExprs           whether expressions which cannot be converted
   *                                       may be omitted from the resulting expression
   * @return parquet filter predicate
   */
  public FilterPredicate getFilterPredicate(ParquetGroupScan parquetGroupScan,
                                            LogicalExpression filterExpr,
                                            UdfUtilities udfUtilities,
                                            FunctionImplementationRegistry functionImplementationRegistry,
                                            OptionManager optionManager,
                                            boolean omitUnsupportedExprs) {

    TupleMetadata types = parquetGroupScan.getColumnMetadata();
    if (types == null) {
      throw new UnsupportedOperationException("At least one schema source should be available.");
    }

    Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    // adds implicit or partition columns if they weren't added before.
    if (parquetGroupScan.supportsFileImplicitColumns()) {
      for (SchemaPath schemaPath : schemaPathsInExpr) {
        if (parquetGroupScan.isImplicitOrPartCol(schemaPath, optionManager) && SchemaPathUtils.getColumnMetadata(schemaPath, types) == null) {
          types.add(MaterializedField.create(schemaPath.getRootSegmentPath(), Types.required(TypeProtos.MinorType.VARCHAR)));
        }
      }
    }

    ErrorCollector errorCollector = new ErrorCollectorImpl();
    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
      filterExpr, types, errorCollector, functionImplementationRegistry);

    if (errorCollector.hasErrors()) {
      logger.error("{} error(s) encountered when materialize filter expression : {}",
        errorCollector.getErrorCount(), errorCollector.toErrorString());
      return null;
    }
    logger.debug("materializedFilter : {}", ExpressionStringBuilder.toString(materializedFilter));

    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
    return FilterBuilder.buildFilterPredicate(materializedFilter, constantBoundaries, udfUtilities, omitUnsupportedExprs);
  }


/*
 *
 *   End of copied code
 *
 */

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

    try {

      LogicalExpression filterExpr = rowGroupScan.getFilter();
      Path selectionRoot = rowGroupScan.getSelectionRoot();
      // Runtime pruning: To avoid recomputing metadata objects for each row-group in case they use the same file
      // keep the following objects computed earlier
      Path prevRowGroupPath = null;
      Metadata_V3.ParquetTableMetadata_v3 tableMetadataV3 = null;
      Metadata_V3.ParquetFileMetadata_v3 mdfv3 = null;
      FileSelection fileSelection = null;
      ParquetTableMetadataProviderImpl metadataProvider = null;

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

          int rowGroupIndex = rowGroup.getRowGroupIndex();
          long footerRowCount = footer.getBlocks().get(rowGroupIndex).getRowCount();

          //
          //   If a filter is given (and it is not just "TRUE") - then use it to perform run-time pruning
          //
          if ( filterExpr != null && ! (filterExpr instanceof ValueExpressions.BooleanExpression)  ) { // skip when no filter or filter is TRUE

            if ( timer != null ) {  // restart the timer, if tracing
              timer.reset();
              timer.start();
            }

            // Initialize path specific metadata etc, when a new file, or at the first time
            if ( ! rowGroup.getPath().equals(prevRowGroupPath) ) {
              // Get the table metadata (V3)
              tableMetadataV3 = Metadata.getParquetTableMetadata(fs, rowGroup.getPath().toString(), readerConfig);

              // The file status for this file
              FileStatus fileStatus = fs.getFileStatus(rowGroup.getPath());
              List<FileStatus> listFileStatus = new ArrayList<>(Arrays.asList(fileStatus));
              List<Path> listRowGroupPath = new ArrayList<>(Arrays.asList(rowGroup.getPath()));
              List<ReadEntryWithPath> entries = new ArrayList<>(Arrays.asList(new ReadEntryWithPath(rowGroup.getPath())));
              fileSelection = new FileSelection(listFileStatus, listRowGroupPath, selectionRoot);

              metadataProvider = new ParquetTableMetadataProviderImpl(entries, selectionRoot, fileSelection.cacheFileRoot, readerConfig, fs,false);
              // The file metadata (V3 - for all columns)
              mdfv3 = getParquetFileMetadata_v3(tableMetadataV3, fileStatus, fs, true, null, readerConfig);

              prevRowGroupPath = rowGroup.getPath(); // for next time
            }

            // MetadataBase.ParquetTableMetadataBase tableMetadata = tableMetadataV3.clone();

            MetadataBase.RowGroupMetadata rowGroupMetadata = mdfv3.getRowGroups().get(rowGroup.getRowGroupIndex());
            // ***** MetadataBase.RowGroupMetadata rowGroupMetadata = getRowGroupMetadata(tableMetadataV3,fileStatus, rowGroupIndex, fs, readerConfig);

            Map<SchemaPath, ColumnStatistics> columnsStatistics = ParquetTableMetadataUtils.getRowGroupColumnStatistics(tableMetadataV3, rowGroupMetadata);

            List<SchemaPath> columns = columnsStatistics.keySet().stream().collect(Collectors.toList());

            ParquetGroupScan pgs = new ParquetGroupScan( context.getQueryUserName(), metadataProvider, fileSelection, columns, readerConfig, filterExpr);

            FilterPredicate filterPredicate = getFilterPredicate(pgs, filterExpr, context, (FunctionImplementationRegistry) context.getFunctionRegistry(), context.getOptions(), true);

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
              if (firstRowGroup == null) {  // keep first RG, in case all row groups are pruned
                firstRowGroup = rowGroup;
                firstFooter = footer;
              }
              continue; // This Row group does not comply with the filter - prune it out and check the next Row Group
            }
          }

          mapWithMaxColumns = createReaderAndImplicitColumns(context, rowGroupScan, oContext, columnExplorer, readers, implicitColumns, mapWithMaxColumns, rowGroup,
           fs, footer, false);
      }

      // in case all row groups were pruned out - create a single reader for the first one (so that the schema could be returned)
      if ( readers.size() == 0 && firstRowGroup != null ) {
        logger.trace("All row groups were pruned out. Returning the first: {} (row count {}) for its schema", firstRowGroup.getPath(), firstRowGroup.getNumRecordsToRead());
        DrillFileSystem fs = fsManager.get(rowGroupScan.getFsConf(firstRowGroup), firstRowGroup.getPath());
        mapWithMaxColumns = createReaderAndImplicitColumns(context, rowGroupScan, oContext, columnExplorer, readers, implicitColumns, mapWithMaxColumns, firstRowGroup, fs,
          firstFooter, true);
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

  private Map<String, String> createReaderAndImplicitColumns(ExecutorFragmentContext context, AbstractParquetRowGroupScan rowGroupScan, OperatorContext oContext, ColumnExplorer columnExplorer, List<RecordReader> readers, List<Map<String, String>> implicitColumns, Map<String, String> mapWithMaxColumns, RowGroupReadEntry rowGroup, DrillFileSystem fs, ParquetMetadata footer, boolean readSchemaOnly) {
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
          readSchemaOnly ? 0 : rowGroup.getNumRecordsToRead(), // read 0 rows when only the schema is needed
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
