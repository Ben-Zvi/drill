package org.apache.drill.exec.ops;

import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;



public class FragmentSharedMemory {
  private Semaphore sem = new Semaphore(1);

  public class OperatorSharedMemory {
    private Semaphore opSem = new Semaphore(1);
    private int opCount = 0;
    Object opShared[] = new Object[4];
    Object sh2 = null;

    public boolean isZero() { return opCount == 0; }
    public void inc() { opCount++; }
    public void dec() { opCount--; }
    public int getCount() { return opCount; }
    // public Semaphore getSemaphore() { return sem; }
    public void lock() throws DrillRuntimeException {
      try { opSem.acquire(); }
      catch (InterruptedException ie) { throw new DrillRuntimeException(ie); }
    }
    public void release() { opSem.release(); }
    public void setObject( Object ob, int offset ) { opShared[offset] = ob; }
    public Object getObject(int offset) { return opShared[offset]; }
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

  public synchronized OperatorSharedMemory getOrAddOp(int op) {
    OperatorSharedMemory osm = opMap.get(new Integer(op));
    if ( null != osm ) { return osm; } // found it
    opMap.put(new Integer(op), new OperatorSharedMemory());
    return opMap.get(new Integer(op));
  }

}
