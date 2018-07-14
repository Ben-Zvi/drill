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
package org.apache.drill.exec.physical.impl.join;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.MockRecordBatch;
import org.apache.drill.exec.physical.unit.PhysicalOpUnitTestBase;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.mock.MockStorePOP;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

// import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *  Unit tests of the Hash Join getting various outcomes as input
 *  with uninitialized vector containers
 */
@Category(OperatorTest.class)
public class TestHashJoinOutcome extends PhysicalOpUnitTestBase {

  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestHashJoinOutcome.class);

  // input batch schemas
  private static TupleSchema inputSchemaRight;
  private static TupleSchema inputSchemaLeft;
  private static BatchSchema batchSchemaRight;
  private static BatchSchema batchSchemaLeft;

  // Input containers -- where row count is not set for the 2nd container !!
  private List<VectorContainer> uninitialized2ndInputContainersRight = new ArrayList<>(5);
  private List<VectorContainer> uninitialized2ndInputContainersLeft = new ArrayList<>(5);

  private RowSet.SingleRowSet emptyInputRowSetRight;
  private RowSet.SingleRowSet emptyInputRowSetLeft;

  // default Non-Empty input RowSets
  private RowSet.SingleRowSet nonEmptyInputRowSetRight;
  private RowSet.SingleRowSet nonEmptyInputRowSetLeft;

  // List of incoming containers
  private final List<VectorContainer> inputContainerRight = new ArrayList<>(5);
  private final List<VectorContainer> inputContainerLeft = new ArrayList<>(5);

  // List of incoming IterOutcomes
  private final List<RecordBatch.IterOutcome> inputOutcomesRight = new ArrayList<>(5);
  private final List<RecordBatch.IterOutcome> inputOutcomesLeft = new ArrayList<>(5);

  @BeforeClass
  public static void setUpBeforeClass() {
    inputSchemaRight = (TupleSchema) new SchemaBuilder()
      .add("rightcol", TypeProtos.MinorType.INT)
      .buildSchema();
    batchSchemaRight = inputSchemaRight.toBatchSchema(BatchSchema.SelectionVectorMode.NONE);
    inputSchemaLeft = (TupleSchema) new SchemaBuilder()
      .add("leftcol", TypeProtos.MinorType.INT)
      .buildSchema();
    batchSchemaLeft = inputSchemaLeft.toBatchSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  private void prepareUninitContainers(List<VectorContainer> emptyInputContainers,
                                       BatchSchema batchSchema) {
    BufferAllocator allocator = operatorFixture.getFragmentContext().getAllocator();

    VectorContainer vc1 = new VectorContainer(allocator, batchSchema);
    // set for first vc (with OK_NEW_SCHEMA) because record count is checked at AbstractRecordBatch.next
    vc1.setRecordCount(0);
    VectorContainer vc2 = new VectorContainer(allocator, batchSchema);
    // Note - Uninitialized: Record count NOT SET for vc2 !!
    emptyInputContainers.add(vc1);
    emptyInputContainers.add(vc2);
  }

  @Before
  public void beforeTest() throws Exception {

    prepareUninitContainers(uninitialized2ndInputContainersLeft, batchSchemaLeft);

    prepareUninitContainers(uninitialized2ndInputContainersRight, batchSchemaRight);

    nonEmptyInputRowSetRight = operatorFixture.rowSetBuilder(inputSchemaRight)
      .addRow(123)
      .build();
    nonEmptyInputRowSetLeft = operatorFixture.rowSetBuilder(inputSchemaLeft)
      .addRow(123)
      .build();

    // Prepare various (empty/non-empty) containers for each side of the join
    emptyInputRowSetLeft = operatorFixture.rowSetBuilder(inputSchemaLeft).build();
    emptyInputRowSetRight = operatorFixture.rowSetBuilder(inputSchemaRight).build();

    inputContainerRight.add(emptyInputRowSetRight.container());
    inputContainerRight.add(nonEmptyInputRowSetRight.container());

    inputContainerLeft.add(emptyInputRowSetLeft.container());
    inputContainerLeft.add(nonEmptyInputRowSetLeft.container());

    final PhysicalOperator mockPopConfig = new MockStorePOP(null);
    mockOpContext(mockPopConfig, 0, 0);
  }

  @After
  public void afterTest() {
    emptyInputRowSetRight.clear();
    emptyInputRowSetLeft.clear();
    nonEmptyInputRowSetRight.clear();
    nonEmptyInputRowSetLeft.clear();
    inputContainerRight.clear();
    inputOutcomesRight.clear();
    inputContainerLeft.clear();
    inputOutcomesLeft.clear();
  }

  /**
   *  Run the Hash Join where one side has an uninitialized container (the 2nd one)
   * @param innerEmpty True if the right side is the uninitialized
   * @param specialOutcome What outcome the uninitialized container has
   * @param expectedOutcome what result outcome is expected
   */
  private void testHashJoinOutcomes(boolean innerEmpty, RecordBatch.IterOutcome specialOutcome,
                                    RecordBatch.IterOutcome expectedOutcome) {

    inputOutcomesLeft.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomesLeft.add( innerEmpty ? RecordBatch.IterOutcome.OK : specialOutcome);

    inputOutcomesRight.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomesRight.add( innerEmpty ? specialOutcome : RecordBatch.IterOutcome.OK);

    final MockRecordBatch mockInputBatchRight = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      innerEmpty ? uninitialized2ndInputContainersRight : inputContainerRight,
      inputOutcomesRight, batchSchemaRight);
    final MockRecordBatch mockInputBatchLeft = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      innerEmpty ? inputContainerLeft : uninitialized2ndInputContainersLeft,
      inputOutcomesLeft, batchSchemaLeft);

    List<JoinCondition> conditions = Lists.newArrayList();

    conditions.add(new JoinCondition( SqlKind.EQUALS.toString(),
      FieldReference.getWithQuotedRef("leftcol"),
      FieldReference.getWithQuotedRef("rightcol")));

    HashJoinPOP hjConf = new HashJoinPOP(null, null, conditions, JoinRelType.INNER);

    HashJoinBatch hjBatch = new HashJoinBatch(hjConf,operatorFixture.getFragmentContext(), mockInputBatchLeft, mockInputBatchRight );

    assertTrue(hjBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA ); // verify first outcome
    assertTrue(hjBatch.next() == expectedOutcome ); // verify expected outcome
  }

  @Test
  public void testHashJoinStopOutcomeRightSide() {
    testHashJoinOutcomes(true, RecordBatch.IterOutcome.STOP, RecordBatch.IterOutcome.STOP);
  }

  @Test
  public void testHashJoinStopOutcomeLeftSide() {
    testHashJoinOutcomes(false, RecordBatch.IterOutcome.STOP, RecordBatch.IterOutcome.STOP);
  }

  @Test
  public void testHashJoinNoneOutcomeRightSide() {
    testHashJoinOutcomes(true, RecordBatch.IterOutcome.NONE, RecordBatch.IterOutcome.NONE);
  }

  @Test
  public void testHashJoinNoneOutcomeLeftSide() {
    testHashJoinOutcomes(false, RecordBatch.IterOutcome.NONE, RecordBatch.IterOutcome.NONE);
  }
}
