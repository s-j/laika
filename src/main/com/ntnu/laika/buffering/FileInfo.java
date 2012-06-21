package com.ntnu.laika.buffering;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public final class FileInfo {
    private File file;
    private FileChannel channel;


    public FileInfo(File file, FileChannel channel) {
        this.file = file;
        this.channel = channel;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public FileChannel getChannel() {
        return channel;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
    }
}
