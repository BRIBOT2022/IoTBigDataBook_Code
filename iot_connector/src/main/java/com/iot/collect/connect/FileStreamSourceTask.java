package com.iot.collect.connect;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    //日志对象，可进行日志的控制台打印
    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    // schema: 模式、方案，这里代表消息数据的类型，这里默认String类型
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    //用于存储要读取的文件名
    private String filename;
    //输入流
    private InputStream stream;
    private BufferedReader reader = null;

    //缓存数据流的字符数组
    private char[] buffer;
    //offset用于记录buffer的偏移量
    private int offset = 0;
    //消息的主题
    private String topic = null;
    //每批消息的最大容量
    private int batchSize = FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE;

    //流的偏移量
    private Long streamOffset;

    //构造器，调用以下重载的构造器，默认创建一个1024长度的字符数组
    public FileStreamSourceTask() {
        this(1024);
    }

    /* visible for testing */
    //构造器，其中根据传入的大小，初始化字符数组buffer
    FileStreamSourceTask(int initialBufferSize) {
        buffer = new char[initialBufferSize];
    }

    //获取当前连接器的版本
    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

    //获取一些配置的参数等信息，来对当前对象的属性进行初始化
    @Override
    public void start(Map<String, String> props) {
        //从properties中获取文件名
        filename = props.get(FileStreamSourceConnector.FILE_CONFIG);

        //若未获取到文件名
        if (filename == null || filename.isEmpty()) {
            //获取一个标准输入流
            stream = System.in;
            // Tracking offset for stdin doesn't make sense
            streamOffset = null;
            //根据UTF-8字符集将stream字节输入流转换为字符流
            reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        }

        // Missing topic or parsing error is not possible because we've parsed the config in the
        // Connector
        //从properties中获取主题
        topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
        //从properties中获取任务中每批能够传递数据的大小
        batchSize = Integer.parseInt(props.get(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG));
    }

    // poll: 轮询
    @Override //SourceRecord是用来封装消息的数据结构，返回一个SourceRecord的List集合
    public List<SourceRecord> poll() throws InterruptedException {
        // 如果输入流为空，即第一次轮询，要先获取输入流以及Reader;
        // 如果是非第一次调用，则可以直接通过Reader读取数据，不需要重新再获取流以及Reader
        if (stream == null) {
            try {
                //根据文件路径获取一个输入流
                stream = Files.newInputStream(Paths.get(filename));

                //从上下文对象context中获取之前的偏移量，在任务失败时用于恢复
                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));

                if (offset != null) {
                    //获取最后记录的偏移量
                    Object lastRecordedOffset = offset.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        //偏移量类型出错, 必须是Long类型
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordedOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        long skipLeft = (Long) lastRecordedOffset;
                        //跳过offset位置之前的内容，从offset后面开始读取数据
                        while (skipLeft > 0) {
                            try {
                                //从stream输入流的当前位置跳过skipLeft个字节，返回实际跳过的字节数
                                long skipped = stream.skip(skipLeft);
                                //有可能一次没有全部跳过，所以将实际跳过的字节减去，循环直到全部跳过
                                skipLeft -= skipped;
                            } catch (IOException e) {
                                log.error("Error while trying to seek to previous offset in file {}: ", filename, e);
                                throw new ConnectException(e);
                            }
                        }
                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                    //将最后一条记录的offset作为流此时的偏移量，如果lastRecordedOffset为空，则streamOffset为0
                    streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
                } else {
                    //如果offset集合为空，则streamOffset为0
                    streamOffset = 0L;
                }
                //根据UTF-8字符集将stream字节输入流转换为字符流
                reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                log.debug("Opened {} for reading", logFilename());
            } catch (NoSuchFileException e) {
                log.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", logFilename());
                synchronized (this) {
                    //阻塞当前线程1秒并释放锁
                    this.wait(1000);
                }
                return null;
            } catch (IOException e) {
                log.error("Error while trying to open file {}: ", filename, e);
                throw new ConnectException(e);
            }
        }

        // Unfortunately we can't just use readLine() because it blocks in an uninterruptible way.
        // Instead we have to manage splitting lines ourselves, using simple backoff when no new data
        // is available.
        //如果已经获取到了Reader，代表此次调用不是第一次轮询，直接执行以下操作
        try {
            final BufferedReader readerCopy;
            //因为可能是多线程运行，所以需要加同步代码块，同步地获取临界资源reader
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null)
                //如果当前线程没有抢到锁，则直接返回null
                return null;

            //创建一个ArrayList，后边用来保存SourceRecord
            ArrayList<SourceRecord> records = null;

            //nread用于存储每次读取的字符数的变量
            int nread = 0;
            //当readerCopy的缓冲区不为空，或者底层字符流准备就绪，则循环读取数据
            while (readerCopy.ready()) {
                //从buffer数组的第[offset]个字符开始，存储“buffer.length - offset”个读取到的字符
                //如果是第一次读取，则offset为0，即从buffer第一个元素开始存储，直到将buffer填满
                //如果不是第一次读取，代表offset有值，则从buffer的offset位置之后开始存储，将buffer数组填满
                nread = readerCopy.read(buffer, offset, buffer.length - offset);
                log.trace("Read {} bytes from {}", nread, logFilename());

                //如果此次成功读取到数据
                if (nread > 0) {
                    //更新offset的位置
                    offset += nread;
                    //定义一个line变量，来存储每次读取到的一行数据
                    String line;
                    //定义一个变量foundOneLine，来记录是否成功读取到了一行数据
                    boolean foundOneLine = false;
                    //开始按行读取数据，并将封装后的数据添加到List集合中
                    do {
                        //读取buffer内一行的内容
                        line = extractLine();
                        if (line != null) {
                            //将foundOneLine置为true，代表成功读取到了一行数据
                            foundOneLine = true;
                            log.trace("Read a line from {}", logFilename());
                            if (records == null)
                                //如果records为空，则实例化一个ArrayList对象
                                records = new ArrayList<>();
                            //将各种数据封装成一个SourceRecord，添加到List集合records中
                            records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, null,
                                    null, null, VALUE_SCHEMA, line, System.currentTimeMillis()));

                            //当records 大于 设置的每批指定的大小时，则直接返回records集合
                            if (records.size() >= batchSize) {
                                return records;
                            }
                        }
                    } while (line != null);

                    //如果没有读取到任何一行数据，同时buffer偏移量刚好等于buffer的长度时（即当前buffer长度不足以容纳一行）
                    if (!foundOneLine && offset == buffer.length) { //扩充字符数组buffer的长度
                        char[] newbuf = new char[buffer.length * 2];
                        //将buffer中的数据拷贝到newbuf数组中
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        log.info("Increased buffer from {} to {}", buffer.length, newbuf.length);
                        buffer = newbuf;
                    }
                }
            }


            if (nread <= 0)
                synchronized (this) {
                    //阻塞当前线程1秒并释放锁，即这1秒内先不返回records的结果
                    this.wait(1000);
                }

            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    //提取一行数据
    private String extractLine() {
        //首先初始化两个变量，用于后面进行逻辑判断
        //until代表一行末尾的偏移量索引; newStart代表当前行的下一行的开始的偏移量
        int until = -1, newStart = -1;

        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') { //遇到换行符的情况
                //使用until存储当前行的末尾的字符
                until = i;
                //使用newStart存储当前行的新起一行的第一个字符
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') { //遇到回车符的情况
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;
                //使用until存储当前行的末尾的字符
                until = i;
                //如果回车符的下一个字符是换行符，则使换行符的下一个字符作为newStart的值
                //即新一行的开始
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        //until != -1 说明此时until成功记录到一行数据的末尾字符的索引
        if (until != -1) {
            //读取当前行中的数据，存储到一个字符串中
            String result = new String(buffer, 0, until);
            //在buffer中，将读取到的字符从数组中清除，同时将buffer后面的字符前移（前移了newStart个位置）
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            //调整buffer的偏移量offset位置值（因为后面字符前移，所以偏移量也要前移）
            offset = offset - newStart;
            if (streamOffset != null)
                //添加流偏移量的值（newStart值刚好是本次读取的字符数）
                streamOffset += newStart;
            return result;
        } else {
            return null;
        }
    }

    //停止读入任务
    @Override
    public void stop() {
        //打印trace级别的日志
        log.trace("Stopping");
        synchronized (this) {
            try {
                //如果输入流不为空，同时输入流不为标准输入流
                if (stream != null && stream != System.in) {
                    //关闭输入流资源
                    stream.close();
                    //打印trace级别的日志
                    log.trace("Closed input stream");
                }
            } catch (IOException e) {
                //打印error级别的日志
                log.error("Failed to close FileStreamSourceTask stream: ", e);
            }
            //唤醒阻塞的线程，使其处于就绪状态
            this.notify();
        }
    }

    //偏移量的key值，为读取的文件名
    private Map<String, String> offsetKey(String filename) {
        //返回一个单例的Map，key为"filename"字符串，value为读取的文件名
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    //偏移量的value，即读取内容的偏移量
    private Map<String, Long> offsetValue(Long pos) {
        //返回一个单例的Map，key为"position"字符串，value为读取内容的偏移量
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    //获取接收源文件的文件名，如果文件名不存在，则返回"stdin"字符串代表标准输入
    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }

    /* visible for testing */
    //获取当前缓存数组buffer的长度，测试时可见
    int bufferSize() {
        return buffer.length;
    }
}
