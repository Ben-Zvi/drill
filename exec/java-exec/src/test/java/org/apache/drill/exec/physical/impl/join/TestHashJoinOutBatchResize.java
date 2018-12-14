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

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
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
import org.apache.drill.test.PhysicalOpUnitTestBase;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.mock.MockStorePOP;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *  Unit tests of the Hash Join getting various outcomes as input
 *  with uninitialized vector containers
 */
@Category(OperatorTest.class)
public class TestHashJoinOutBatchResize extends PhysicalOpUnitTestBase {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestHashJoinOutcome.class);

  // input batch schemas
  private static TupleSchema inputSchemaRight;
  private static TupleSchema inputSchemaLeft;
  private static BatchSchema batchSchemaRight;
  private static BatchSchema batchSchemaLeft;

  // default Non-Empty input RowSets
  private RowSet.SingleRowSet inputRowSetRight;
  private RowSet.SingleRowSet inputRowSetLeft;
  private RowSet.SingleRowSet secondInputRowSetLeft;
  private RowSet.SingleRowSet thirdInputRowSetLeft;
  // and empty ones
  private RowSet.SingleRowSet emptyInputRowSetRight;
  private RowSet.SingleRowSet emptyInputRowSetLeft;

  // List of incoming containers
  private final List<VectorContainer> inputContainerRight = new ArrayList<>(5);
  private final List<VectorContainer> inputContainerLeft = new ArrayList<>(500);

  // List of incoming IterOutcomes
  private final List<RecordBatch.IterOutcome> inputOutcomesRight = new ArrayList<>(5);
  private final List<RecordBatch.IterOutcome> inputOutcomesLeft = new ArrayList<>(700);

  @BeforeClass
  public static void setUpBeforeClass() {
    inputSchemaRight = (TupleSchema) new SchemaBuilder()
      .add("rightcol", TypeProtos.MinorType.INT)
      .add("fatRCol", TypeProtos.MinorType.VARCHAR)
      .buildSchema();
    batchSchemaRight = inputSchemaRight.toBatchSchema(BatchSchema.SelectionVectorMode.NONE);
    inputSchemaLeft = (TupleSchema) new SchemaBuilder()
      .add("leftcol", TypeProtos.MinorType.INT)
      .add("fatLCol", TypeProtos.MinorType.VARCHAR)
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

    // Prepare rowsets
    inputRowSetRight = operatorFixture.rowSetBuilder(inputSchemaRight)
      .addRow(123,
        "01234567890")
      .build();
    inputRowSetLeft = operatorFixture.rowSetBuilder(inputSchemaLeft)
      .addRow(123, // 460 char long
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
      .build();
    secondInputRowSetLeft = operatorFixture.rowSetBuilder(inputSchemaLeft)
      .addRow(123, // 460 char long
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
      .build();
    RowSetBuilder rsb = operatorFixture.rowSetBuilder(inputSchemaLeft);
    rsb.addRow(123, // 460 char long
      "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");

    for ( int i = 0; i < 40; i++ ) {
      rsb.addRow(123,"ab");
    }
    thirdInputRowSetLeft = rsb.build();


    emptyInputRowSetLeft = operatorFixture.rowSetBuilder(inputSchemaLeft).build();
    emptyInputRowSetRight = operatorFixture.rowSetBuilder(inputSchemaRight).build();

    // Prepare various (empty/non-empty) containers (and their outcomes) for each side of the join

    // Right side - empty (new schema) first
    inputContainerRight.add(emptyInputRowSetRight.container());
    inputOutcomesRight.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA); // empty
    inputContainerRight.add(inputRowSetRight.container());
    inputOutcomesRight.add(RecordBatch.IterOutcome.OK);   // first non-empty right
    inputOutcomesRight.add(RecordBatch.IterOutcome.NONE);

    // Left side
    inputContainerLeft.add(emptyInputRowSetLeft.container());
    inputOutcomesLeft.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA); // empty
    inputContainerLeft.add(inputRowSetLeft.container());
    inputOutcomesLeft.add(RecordBatch.IterOutcome.OK); // 1st left
    inputContainerLeft.add(secondInputRowSetLeft.container());
    inputOutcomesLeft.add(RecordBatch.IterOutcome.OK);   // 2nd left
    inputContainerLeft.add(thirdInputRowSetLeft.container());
    inputOutcomesLeft.add(RecordBatch.IterOutcome.OK);  // 3rd left
    inputOutcomesLeft.add(RecordBatch.IterOutcome.NONE);


    final PhysicalOperator mockPopConfig = new MockStorePOP(null);
    mockOpContext(mockPopConfig, 0, 0);
  }

  @After
  public void afterTest() {
    inputRowSetRight.clear();
    inputRowSetLeft.clear();
    secondInputRowSetLeft.clear();
    thirdInputRowSetLeft.clear();
    emptyInputRowSetLeft.clear();
    emptyInputRowSetRight.clear();
    inputContainerRight.clear();
    inputOutcomesRight.clear();
    inputContainerLeft.clear();
    inputOutcomesLeft.clear();
  }

  /**
   *  Run the Hash Join where one side has an uninitialized container (the 2nd one)
   */
  @Test
  public void TestOutBatchResize() {

    operatorFixture.getOptionManager().setLocalOption("drill.exec.memory.operator.output_batch_size", 2048);

    final MockRecordBatch mockInputBatchRight = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext, inputContainerRight,
      inputOutcomesRight, batchSchemaRight);
    final MockRecordBatch mockInputBatchLeft = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext, inputContainerLeft,
      inputOutcomesLeft, batchSchemaLeft);

    List<JoinCondition> conditions = Lists.newArrayList();

    conditions.add(new JoinCondition( SqlKind.EQUALS.toString(),
      FieldReference.getWithQuotedRef("leftcol"),
      FieldReference.getWithQuotedRef("rightcol")));

    HashJoinPOP hjConf = new HashJoinPOP(null, null, conditions, JoinRelType.INNER);

    HashJoinBatch hjBatch = new HashJoinBatch(hjConf,operatorFixture.getFragmentContext(), mockInputBatchLeft, mockInputBatchRight );

    RecordBatch.IterOutcome gotOutcome = hjBatch.next();
    System.out.println(hjBatch.getRecordCount());
    assertTrue("No new schema",gotOutcome == RecordBatch.IterOutcome.OK_NEW_SCHEMA );

    gotOutcome = hjBatch.next();
    assertTrue("No OK", gotOutcome == RecordBatch.IterOutcome.OK); // verify returned outcome
    System.out.println(hjBatch.getRecordCount());

    gotOutcome = hjBatch.next();
    assertTrue("No OK",gotOutcome == RecordBatch.IterOutcome.OK); // verify returned outcome
    System.out.println(hjBatch.getRecordCount());

    gotOutcome = hjBatch.next();
    assertTrue("No OK " + gotOutcome,gotOutcome == RecordBatch.IterOutcome.NONE); // verify returned outcome
    System.out.println(hjBatch.getRecordCount());

  }


}
