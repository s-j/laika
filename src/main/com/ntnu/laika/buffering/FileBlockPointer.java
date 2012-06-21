package com.ntnu.laika.buffering;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public class FileBlockPointer {
    private final int fileNumber;
    private final long blockNumber;

    public FileBlockPointer(int fileNumber, long blockNumber) {
        this.fileNumber = fileNumber;
        this.blockNumber = blockNumber;
    }

    public FileBlockPointer next() {
        return new FileBlockPointer(fileNumber, blockNumber + 1);
    }

    public FileBlockPointer previous() {
        return new FileBlockPointer(fileNumber, blockNumber - 1);
    }

    public int getFileNumber() {
        return fileNumber;
    }

    public long getBlockNumber() {
        return blockNumber;
    }

    @Override
    public int hashCode() {
        return fileNumber ^ (int) (blockNumber % (long) Integer.MAX_VALUE);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof FileBlockPointer) {
            return equals((FileBlockPointer) obj);
        }
        return false;
    }

    @Override
    public String toString() {
        return fileNumber + " " + blockNumber;
    }

    public boolean equals(FileBlockPointer other) {
        return other != null && other.fileNumber == fileNumber && other.blockNumber == blockNumber;
    }
}
