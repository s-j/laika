package com.ntnu.laika.utils;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A O(1) blocking linked list implementation. Backed by a linked hash map to allow O(1) removal of
 * and membedrship checks.
 *
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public class LookupBlockingFifoQueue<V>
        extends AbstractQueue<V> implements BlockingQueue<V> {

    private final Object mutex;

    private AtomicReference<Map<V, V>> backingHashMap;


    public LookupBlockingFifoQueue(Object mutex) {
        this.mutex = mutex;
        resetBackingMap();
    }

    private void resetBackingMap() {
        synchronized (mutex) {
            backingHashMap = new AtomicReference<Map<V, V>>(Collections.synchronizedMap(new LinkedHashMap<V, V>()));
        }
    }

    public boolean offer(V e) {
        try {
            put(e);
        } catch (InterruptedException err) {
            err.printStackTrace();
            return false;
        }
        return true;
    }

    public void put(V e) throws InterruptedException {
        synchronized (mutex) {
            backingHashMap.get().put(e, e);
            mutex.notifyAll();
        }
    }

    public boolean offer(V e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    public V take() throws InterruptedException {
        synchronized (mutex) {
            while (backingHashMap.get().size() < 1) {
                mutex.wait();
            }
            V elem = safeGetFirst();
            assert elem != null;
            backingHashMap.get().remove(elem);
            return elem;
        }
    }

    public V poll(long timeout, TimeUnit unit) throws InterruptedException {
        long started = System.currentTimeMillis();
        long wait = unit.toMillis(timeout);
        long rest = wait;
        synchronized (mutex) {
            while (size() < 1 && rest > 0) {
                rest = wait - (System.currentTimeMillis() - started);
                mutex.wait(rest);
            }
            return poll();
        }
    }

    public V poll() {
        synchronized (mutex) {
            V elem = safeGetFirst();
            if (elem != null) {
                backingHashMap.get().remove(elem);
            }
            return elem;
        }
    }

    public V peek() {
        return safeGetFirst();
    }

    @Override
    public boolean remove(Object o) {
        synchronized (mutex) {
            return backingHashMap.get().remove(o) != null;
        }
    }

    @Override
    public boolean contains(Object o) {
        synchronized (mutex) {
            return backingHashMap.get().containsKey(o);
        }
    }

    /**
     * Gets the first element of the backing map.
     *
     * @return The oldest element in the map (ornull if empty)
     */
    private V safeGetFirst() {
        synchronized (mutex) {
            Iterator<Map.Entry<V, V>> it = backingHashMap.get().entrySet().iterator();
            return it.hasNext() ? it.next().getKey() : null;
        }
    }

    @Override
    public int size() {
        synchronized (mutex) {
            return backingHashMap.get().size();
        }
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public int drainTo(Collection<? super V> c) {
        Map<V, V> copy = backingHashMap.getAndSet(Collections.synchronizedMap(new LinkedHashMap<V, V>()));
        int count = 0;
        for (Map.Entry<V, V> entry : copy.entrySet()) {
            c.add(entry.getKey());
            ++count;
        }
        return count;
    }

    public int drainTo(Collection<? super V> c, int maxElements) {
        // Not implemented since it would require to much locking. (or to much skills I guess:))
        throw new NotImplementedException();
    }

    @Override
    public Iterator<V> iterator() {
        throw new NotImplementedException();
    }
}
