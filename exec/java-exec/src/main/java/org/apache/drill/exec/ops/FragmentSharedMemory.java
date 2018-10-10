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

package org.apache.drill.exec.ops;

import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;


public class FragmentSharedMemory {

  public class OperatorSharedMemory {
    private Semaphore opSem;
    private volatile int opWidth;
    Object opShared[];

    public synchronized boolean deactivate() { return 0 == --opWidth; }
    // public Semaphore getSemaphore() { return sem; }
    public void lock() throws DrillRuntimeException {
      try { opSem.acquire(); }
      catch (InterruptedException ie) { throw new DrillRuntimeException(ie); }
    }
    public void release() { opSem.release(); }
    public void setObject( Object ob, int offset ) { opShared[offset] = ob; }
    public Object getObject(int offset) { return opShared[offset]; }

    OperatorSharedMemory () {
      opSem = new Semaphore(1);
      opWidth = getNumMinorFragments();
      opShared = new Object[4];
      opShared[0] = null; // mark as uninitialized
    }
  }

  private Map<Integer, OperatorSharedMemory> opMap = new HashMap<Integer, OperatorSharedMemory>(); /* {
    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean containsKey(Object key) {
      return false;
    }

    @Override
    public boolean containsValue(Object value) {
      return false;
    }

    @Override
    public Semaphore get(Object key) {
      return null;
    }

    @Override
    public Semaphore put(Integer key, Semaphore value) {
      return null;
    }

    @Override
    public Semaphore remove(Object key) {
      return null;
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends Semaphore> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Integer> keySet() {
      return null;
    }

    @Override
    public Collection<Semaphore> values() {
      return null;
    }

    @Override
    public Set<Entry<Integer, Semaphore>> entrySet() {
      return null;
    }
  };
  private int count = 0;
  Object shared[] = new Object[4];
  Object sh2 = null;




  public boolean isZero() { return count == 0; }
  public void inc() { count++; }
  public void dec() { count--; }
  public int getCount() { return count; }
  // public Semaphore getSemaphore() { return sem; }
  public void lock() throws DrillRuntimeException {
    try { sem.acquire(); }
    catch (InterruptedException ie) { throw new DrillRuntimeException(ie); }
  }
  public void release() { sem.release(); }
  public void setObject( Object ob, int offset ) { shared[offset] = ob; }
  public Object getObject(int offset) { return shared[offset]; } */

  // Each Shared-Memory (per a major fragment) will set the number of its minor fragments (per this node)
  private Semaphore sem = new Semaphore(1);
  private volatile int numMinorFragments; // i.e., the width

  public void lock() { try {sem.acquire();} catch (InterruptedException ie) { return; } }
  public void release() { sem.release(); }

  public void incNumMinorFragments() {
    try {
      sem.acquire();
      numMinorFragments++;
      sem.release();
    } catch (InterruptedException ie) { return; }
  }
  public void decNumMinorFragments() {
    try {
      sem.acquire();
      numMinorFragments--;
      sem.release();
    } catch (InterruptedException ie) { return; }
  }
  public int getNumMinorFragments() {
    try {
      int result;
      sem.acquire();
      result = numMinorFragments;
      sem.release();
      return result;
    } catch (InterruptedException ie) { return Integer.MAX_VALUE; }
  }


  public synchronized OperatorSharedMemory getOrAddOp(int op) {
    OperatorSharedMemory osm = opMap.get(new Integer(op));
    // if ( null != osm ) { System.out.format("Found osm %x for op %d\n",System.identityHashCode(osm), op); } // found it
    if ( null != osm ) { return osm; } // found it
    opMap.put(new Integer(op), new OperatorSharedMemory());

    // System.out.format("Creating osm %x for op %d\n",System.identityHashCode(opMap.get(new Integer(op))), op);

    return opMap.get(new Integer(op));
  }

}
