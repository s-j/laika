package com.ntnu.laika.buffering;

import com.ntnu.laika.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public class Buffer {

    private static final class BufferMutex {
    }

    private final Object mutex = new BufferMutex();
    private final ReentrantReadWriteLock readerWriterLock = new ReentrantReadWriteLock();

    private boolean pinned;
    private boolean dirty;
    private boolean sourceDeleted;

    private final AtomicInteger pinnedCount = new AtomicInteger();

    private FileInfo fileInfo;
    private FileBlockPointer blockPointer;

    private FileBlockPointer bufferedBlock;
    private final ByteBuffer buffer;
    private final int blockSize;

    public Buffer(int blockSize) {
        assert blockSize >= Constants.BUFFER_BLOCK_SIZE;
        this.blockSize = blockSize;
        buffer = ByteBuffer.allocateDirect(blockSize).order(ByteOrder.LITTLE_ENDIAN);
    }


    /**
     * Loads the data from file and into the buffer.
     *
     * @throws IOException If there are some error while reading the file.
     */
    public void load() throws IOException {
        synchronized (mutex) {
            if (bufferedBlock != null && bufferedBlock.equals(blockPointer)
                    || blockPointer == null) {
                return;
            }
            readerWriterLock.writeLock().lock();
            try {
                buffer.clear();
                fileInfo.getChannel().read(buffer, blockPointer.getBlockNumber() * blockSize);
                buffer.flip();
                buffer.limit(blockSize);
                bufferedBlock = blockPointer;
            }
            finally {
                readerWriterLock.writeLock().unlock();
            }
        }
    }

    /**
     * Flushes the buffer to file.
     *
     * @throws IOException If there were some error while writing to file.
     */
    public void flush() throws IOException {
        synchronized (mutex) {
            if (dirty) {
                readerWriterLock.readLock().lock();
                try {
                    buffer.position(0); // flush entire buffer
                    buffer.limit(blockSize);
                    fileInfo.getChannel().write(buffer, blockPointer.getBlockNumber() * blockSize);
                    buffer.clear();
                }
                finally {
                    readerWriterLock.readLock().unlock();
                }
            }
        }
    }

    /**
     * Returns the underlying byte buffer for modification and reading. The caller is responsible for synchronization.
     * The position, mark and limit of the buffer is independent of other buffers returned from this method.
     *
     * @return The byte buffer underlying this buffer.
     */
    public ByteBuffer getByteBuffer() {
        return buffer.duplicate();
    }

    /**
     * Returns but does not acquire a reader lock for this buffer.
     *
     * @return Reader lock for buffer.
     */
    public ReentrantReadWriteLock.ReadLock getReadLock() {
        return readerWriterLock.readLock();
    }

    /**
     * Returns but does not acquire a writer lock for this buffer.
     *
     * @return Writer lock for buffer.
     */
    public ReentrantReadWriteLock.WriteLock getWriteLock() {
        return readerWriterLock.writeLock();
    }

    /**
     * Gets if the source file for this buffer is deleted.
     *
     * @return True if the source file is deleted.
     */
    public boolean isSourceDeleted() {
        synchronized (mutex) {
            return sourceDeleted;
        }
    }

    /**
     * Sets if the source file for this buffer is deleted.
     *
     * @param b True if the source file is deleted.
     */
    public void setSourceDeleted(boolean b) {
        synchronized (mutex) {
            sourceDeleted = b;
        }
    }


    public FileBlockPointer getBlockPointer() {
        synchronized (mutex) {
            return blockPointer;
        }
    }

    public void setBlockPointer(FileBlockPointer fileBlockPointer) {
        synchronized (mutex) {
            if (blockPointer != null && fileBlockPointer.getFileNumber() != blockPointer.getFileNumber()) {
                sourceDeleted = false;
            }
            blockPointer = fileBlockPointer;
        }
    }

    public FileInfo getFileInfo() {
        synchronized (mutex) {
            return fileInfo;
        }
    }

    public void setFileInfo(FileInfo fileInfo) {
        synchronized (mutex) {
            //noinspection ObjectEquality
            if (this.fileInfo != null && fileInfo.getChannel() != this.fileInfo.getChannel()) {
                sourceDeleted = false;
            }
            this.fileInfo = fileInfo;
        }
    }

    public boolean isDirty() {
        synchronized (mutex) {
            return dirty;
        }
    }

    public void setIsDirty(boolean dirty) {
        synchronized (mutex) {
            this.dirty = dirty;
        }
    }

    public boolean isPinned() {
        return pinned;
    }

    public void setIsPinned(boolean b) {
        pinned = b;
    }

    public int incrementAndReturnPinnedCount() {
        return pinnedCount.incrementAndGet();
    }

    public int decrementAndReturnPinnedCount() {
        return pinnedCount.decrementAndGet();
    }

}
