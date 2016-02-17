package com.migrate

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.io.LineIterator

/**
 * Created by rubinder on 24/9/15.
 */
class LogReader {

    private final File file;

    private final LineIterator lineIterator;

    private long lastLineRead = -1L;

    public LogReader(final File file)
            throws IOException {
        this.file = file;
        this.lastLineRead = 1L;
        try {
            lineIterator = FileUtils.lineIterator(file, "UTF-8");
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    public long seek(final long line) {
        long lineCount = 1L;
        while((lineIterator != null) && (lineIterator.hasNext()) && (line==-1L || lineCount < line)) {
            lineIterator.nextLine();
            lineCount += 1L;
        }
        // If we got to the end of the file, but haven't read as many
        // lines as we should have, then the requested line number is
        // out of range.
        lastLineRead = lineCount
        if(lineCount < line) {
            lastLineRead = -1
            throw new NoSuchElementException("Invalid line number; " +
                    "out of range.");
        }

        return lineCount;
    }

    /**
     * Closes this IOUtils LineIterator and the underlying
     * input stream reader.
     */
    public void close() {
        LineIterator.closeQuietly(lineIterator);
    }

    /**
     * Returns true of there are any more lines to read in the
     * file.  Otherwise, returns false.
     * @return
     */
    public boolean hasNext() {
        return lineIterator.hasNext();
    }

    /**
     * Read a line of text from this reader.
     * @return
     */
    public String readLine() {
        String ret = null;
        try {
            // If there is nothing more to read with this LineIterator
            // then nextLine() throws a NoSuchElementException.
            ret = new String(lineIterator.nextLine());
            lastLineRead += 1L;
        } catch (NoSuchElementException e) {
            throw e;
        }
        return ret;
    }

    public long getLastLineRead() {
        return lastLineRead;
    }

}
