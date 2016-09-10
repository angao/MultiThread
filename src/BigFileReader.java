import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BigFileReader.java
 * Create on 16/1/19
 *
 * @author leo
 * @version 1.0
 */
public class BigFileReader {
    private int threadSize; //线程数
    private String charset; //字符集
    private int bufferSize; //
    private IHandle handle; //文件操作接口
    private ExecutorService executorService; //管理线程
    private long fileLength; //文件字节数
    private RandomAccessFile randomAccessFile; //
    private Set<StartEndPair> startEndPairs;
    private CyclicBarrier cyclicBarrier; //
    private AtomicLong counter = new AtomicLong(0);

    private BigFileReader(File file, IHandle handle, String charset, int bufferSize, int threadSize) {
        this.fileLength = file.length();
        this.handle = handle;
        this.charset = charset;
        this.bufferSize = bufferSize;
        this.threadSize = threadSize;
        try {
            this.randomAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.executorService = Executors.newFixedThreadPool(threadSize);
        startEndPairs = new HashSet<StartEndPair>();
    }
    public void start() {
        // 每个线程需要读取文件的大小
        long everySize = this.fileLength / this.threadSize;
        try {
            calculateStartEnd(0, everySize);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final long startTime = System.currentTimeMillis();
        cyclicBarrier = new CyclicBarrier(startEndPairs.size(), new Runnable() {
            @Override
            public void run() {
                System.out.println("Use time: " + (System.currentTimeMillis() - startTime));
                System.out.println("All line: " + counter.get());
            }
        });

        for (StartEndPair pair : startEndPairs) {
            System.out.println("Allocation pair:" + pair);
            this.executorService.execute(new SliceReaderTask(pair));
        }
    }

    private void calculateStartEnd(long start, long size) throws IOException {
        if (start > fileLength - 1) {
            return;
        }
        StartEndPair pair = new StartEndPair();
        pair.start = start;
        long endPosition = start + size -1;
        if (endPosition >= fileLength - 1) {
            pair.end = fileLength - 1;
            startEndPairs.add(pair);
            return;
        }
        randomAccessFile.seek(endPosition);
        byte tmp = (byte) randomAccessFile.read();
        while (tmp != '\n' && tmp != '\r') {
            endPosition++;
            if (endPosition >= fileLength - 1) {
                endPosition = fileLength -1;
                break;
            }
            randomAccessFile.seek(endPosition);
            tmp = (byte) randomAccessFile.read();
        }
        pair.end = endPosition;
        startEndPairs.add(pair);

        calculateStartEnd(endPosition+1, size);
    }

    /**
     * 关闭文件,结束线程
     */
    public void shutdown() {
        try {
            this.randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.executorService.shutdown();
    }

    public void handle(byte[] bytes) throws UnsupportedEncodingException {
        String line = null;
        if (this.charset == null) {
            line = new String(bytes);
        } else {
            line = new String(bytes, charset);
        }
        if (line != null && !"".equals(line)) {
            this.handle.handle(line);
            counter.incrementAndGet();
        }
    }

    private class StartEndPair {
        public long start;
        public long end;

        @Override
        public String toString() {
            return "start=" + start + "; end=" + end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StartEndPair pair = (StartEndPair) o;

            if (start != pair.start) return false;
            return end == pair.end;

        }

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }

    private class SliceReaderTask implements Runnable {
        private long start;
        private long sliceSize;
        private byte[] readBuff;

        public SliceReaderTask(StartEndPair pair) {
            this.start = pair.start;
            this.sliceSize = pair.end -pair.start + 1;
            this.readBuff = new byte[bufferSize];
        }

        @Override
        public void run() {
            try {
                MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, start, this.sliceSize);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (int offset = 0; offset < sliceSize; offset+=bufferSize) {
                    int readLength;
                    if (offset + bufferSize <= sliceSize) {
                        readLength = bufferSize;
                    } else {
                        readLength = (int) (sliceSize - offset);
                    }
                    mappedByteBuffer.get(readBuff, 0, readLength);
                    for (int i = 0; i < readLength; i++) {
                        byte tmp = readBuff[i];
                        if (tmp == '\n' || tmp == '\r') {
                            handle(bos.toByteArray());
                            bos.reset();
                        } else {
                            bos.write(tmp);
                        }
                    }
                }
                if (bos.size() > 0) {
                    handle(bos.toByteArray());
                }
                cyclicBarrier.await();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
    public static class Builder {
        private int threadSize = 1;
        private String charset = null;
        private int bufferSize = 1024 * 1024;
        private IHandle handle;
        private File file;

        public Builder(String file, IHandle handle) {
            this.file = new File(file);
            if (!this.file.exists()) {
                throw new IllegalArgumentException("File Not Exist.");
            }
            this.handle = handle;
        }
        public Builder withTreahdSize(int size) {
            this.threadSize = size;
            return this;
        }
        public Builder withCharset(String charset) {
            this.charset = charset;
            return this;
        }
        public Builder withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }
        public BigFileReader build() {
            return new BigFileReader(this.file, this.handle, this.charset, this.bufferSize, this.threadSize);
        }
    }
}
