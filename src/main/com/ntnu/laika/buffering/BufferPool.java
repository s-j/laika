package com.ntnu.laika.buffering;

import com.ntnu.laika.utils.AbstractLifecycleComponent;
import com.ntnu.laika.utils.LookupBlockingFifoQueue;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public class BufferPool extends AbstractLifecycleComponent {

    private static final int FLUSHER_THREAD_PERIOD_MILLI_SECONDS = 200;

    private static final class DeleteMarker {

        private int deletedFileNumber;
        private Set<FileBlockPointer> deletedBuffers;

        private DeleteMarker(int deletedFileNumber, Set<FileBlockPointer> deletedBuffers) {
            this.deletedFileNumber = deletedFileNumber;
            this.deletedBuffers = deletedBuffers;
        }

        public int getDeletedFileNumber() {
            return deletedFileNumber;
        }

        public void setDeletedFileNumber(int deletedFileNumber) {
            this.deletedFileNumber = deletedFileNumber;
        }

        public Set<FileBlockPointer> getBuffers() {
            return deletedBuffers;
        }

        public void setBuffers(Set<FileBlockPointer> buffers) {
            deletedBuffers = buffers;
        }
    }

    private final class FlusherCommand implements Runnable {

        public void run() {
            try {
                //LOG.info("Flushing buffer pool, pinned buffers: " + (numberOfBuffers - cleanBuffers.size() - dirtyBuffers.size()));
                Collection<Buffer> dirty = new ArrayList<Buffer>();
                Collection<DeleteMarker> deleteMarkers = new ArrayList<DeleteMarker>();
                synchronized (mutex) {
                    dirtyBuffers.drainTo(dirty);
                    deleted.drainTo(deleteMarkers);
                }
                for (DeleteMarker deletedMarker : deleteMarkers) {
                    for (FileBlockPointer fbp : deletedMarker.getBuffers()) {
                        synchronized (mutex) {
                            Buffer buffer = buffers.remove(fbp);
                            buffer.setSourceDeleted(true);
                        }
                    }
                }
                deleteMarkers.clear();
                for (Buffer buffer : dirty) {
                    if (!buffer.isSourceDeleted()) {
                        buffer.flush();
                    }
                    buffer.setIsDirty(false);
                    cleanBuffers.put(buffer);
                }

            } catch (InterruptedException e) {
                System.out.println("Flusher command was interrupted.");
                e.printStackTrace();
                System.exit(1);
            } catch (IOException e) {
                System.out.println("Flusher command could not flush.");
                e.printStackTrace();
                System.exit(1);
            } catch (Exception e) {
                System.out.println("Some error occured in the flusher command.");
                e.printStackTrace();
                System.exit(1);
            }
        }

    }

    // Maps file pointers to buffers
    private final Map<FileBlockPointer, Buffer> buffers = new HashMap<FileBlockPointer, Buffer>();
    private final Map<Integer, Set<FileBlockPointer>> buffersReverse = new HashMap<Integer, Set<FileBlockPointer>>();

    private final LookupBlockingFifoQueue<Buffer> dirtyBuffers;
    private final LookupBlockingFifoQueue<Buffer> cleanBuffers;
    private final BlockingQueue<DeleteMarker> deleted;

    // Holds the active files, index by file number
    private final AtomicInteger fileNumber = new AtomicInteger();
    private final Map<Integer, FileInfo> files = new HashMap<Integer, FileInfo>();
    private final int bufferSize;

    // Use a special class to make profiling / debuging easier
    private static final class BufferPoolMutex {

    }

    private final Object mutex = new BufferPoolMutex();
    private ScheduledExecutorService flushExecutor;

    private final int numberOfBuffers;

    public BufferPool(int numberOfBuffers, int bufferSize) {
        this.bufferSize = bufferSize;
        this.numberOfBuffers = numberOfBuffers;
        cleanBuffers = new LookupBlockingFifoQueue<Buffer>(mutex);
        dirtyBuffers = new LookupBlockingFifoQueue<Buffer>(mutex);
        deleted = new LookupBlockingFifoQueue<DeleteMarker>(mutex);
        for (int i = 0; i < numberOfBuffers; i++) {
            cleanBuffers.offer(new Buffer(bufferSize));
        }

    }

    public void start() {
        flushExecutor = new ScheduledThreadPoolExecutor(5);
        flushExecutor.scheduleAtFixedRate(new FlusherCommand(), 0L, (long) BufferPool.FLUSHER_THREAD_PERIOD_MILLI_SECONDS, TimeUnit.MILLISECONDS);
        setIsRunning(true);
    }

    public void stop() {
        setIsRunning(false);
        flushExecutor.shutdown();
        flushExecutor = null;
        new FlusherCommand().run();
        try {
            System.out.println("Forcing changes!");
            forceAll();
        } catch (IOException e) {
            setFailCause(e);
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }


    /**
     * Pins a buffer for the supplied location.
     *
     * @param location The location to create a buffer for.
     * @return A buffer pinned at location.
     * @throws InterruptedException if the thtread is interupted while waiting for a free buffer.
     * @throws IOException          If there were some IO error while pinning the buffer.
     */
    public Buffer pinBuffer(FileBlockPointer location) throws InterruptedException, IOException {
        Buffer buffer;
        synchronized (mutex) {
            buffer = buffers.get(location);
            if (buffer == null) { // take a clean buffer
                buffer = cleanBuffers.take();
                FileBlockPointer oldPointer = buffer.getBlockPointer();
                if (oldPointer != null) {
                    buffers.remove(oldPointer);
                    buffersReverse.get(oldPointer.getFileNumber()).remove(oldPointer); // unregister in reverse lookup
                }
                buffersReverse.get(location.getFileNumber()).add(location); // register buffer in reverse lookup
                buffers.put(location, buffer);
                buffer.setBlockPointer(location);
                buffer.setFileInfo(files.get(location.getFileNumber()));
                buffer.load();
            } else { // remove the buffer from any list
                LookupBlockingFifoQueue<Buffer> list = buffer.isDirty() ? dirtyBuffers : cleanBuffers;
                list.remove(buffer);
            }
        }
        buffer.incrementAndReturnPinnedCount();
        buffer.setIsPinned(true);
        return buffer;
    }


    /**
     * Un-pins the supplied buffer.
     *
     * @param buffer The buffer to be unpinned.
     */
    public void unPinBuffer(Buffer buffer) {
        synchronized(mutex) {
        if (buffer.decrementAndReturnPinnedCount() == 0) {
            buffer.setIsPinned(false);
            if (buffer.isDirty()) {
                dirtyBuffers.offer(buffer);
            } else {
                cleanBuffers.offer(buffer);
            }
        }
        }
    }

    /**
     * Add a file to the buffer. Blocks of this file may be pinned by
     * creating a {@link FileBlockPointer} with the file number returned
     * from this method.
     *
     * @param channel The file channel for the file.
     * @param file    The concrete file object for the file.
     * @return The file number for the file.
     */
    public int registerFile(FileChannel channel, File file) {
        FileInfo fileInfo = new FileInfo(file, channel);
        int newFileNumber = fileNumber.getAndIncrement();
        synchronized (mutex) {
            files.put(newFileNumber, fileInfo);
            buffersReverse.put(newFileNumber, new HashSet<FileBlockPointer>());
        }
        return newFileNumber;
    }

    public void forceAll() throws IOException {
        for (Map.Entry<Integer, FileInfo> e : files.entrySet()) {
            e.getValue().getChannel().force(true);
        }
    }

    /**
     * Deletes the file with the supplied file number.
     *
     * @param fileNumber The file number for the file to be deleted.
     * @throws InterruptedException If the thread were interrupted while trying to delete.
     * @throws IOException          If the file could not be closed or deleted.
     */
    public void deleteFile(int fileNumber) throws InterruptedException, IOException {
        synchronized (mutex) {
            FileInfo fileInfo = files.remove(fileNumber);
            deleted.put(new DeleteMarker(fileNumber, buffersReverse.remove(fileNumber)));
            fileInfo.getChannel().close();
            fileInfo.getFile().delete();
        }
    }
}
