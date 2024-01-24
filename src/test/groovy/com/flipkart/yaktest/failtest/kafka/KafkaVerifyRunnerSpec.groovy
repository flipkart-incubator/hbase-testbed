package com.flipkart.yaktest.failtest.kafka

import kafka.consumer.ConsumerIterator
import kafka.consumer.ConsumerTimeoutException
import kafka.consumer.FetchedDataChunk
import kafka.consumer.KafkaStream
import kafka.consumer.PartitionTopicInfo
import kafka.message.ByteBufferMessageSet
import kafka.message.Message
import kafka.message.NoCompressionCodec$
import kafka.serializer.DefaultDecoder
import kafka.utils.VerifiableProperties

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hbase.thirdparty.com.google.common.base.Charsets
import org.apache.hbase.thirdparty.com.google.common.collect.Queues
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.modules.junit4.PowerMockRunnerDelegate
import org.spockframework.runtime.Sputnik
import scala.collection.JavaConversions
import sep.shade.com.flipkart.yak.sep.SerDe
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto.SepMessage
import sep.shade.com.google.protobuf.ByteString
import spock.lang.Specification

import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([SerDe.class, SepMessage.class])
@PowerMockIgnore(["javax.management.*"])
class KafkaVerifyRunnerSpec extends Specification {

    KafkaVerifyRunner kafkaVerifyRunner
    ConcurrentMap<String, List<Integer>> kafkaAllVersionsMap = new ConcurrentHashMap<>()
    KafkaStream<byte[], byte[]> kafkaStream

    void setup() {
        def partitions = 1
        AtomicBoolean runFlag = new AtomicBoolean(true)
        byte[] byteArray = Bytes.toBytes("test-123")
        byte[] byteArrayInt = Bytes.toBytes("123456")

        ByteString byteString = ByteString.copyFrom(byteArray)
        ByteString byteStringInt = ByteString.copyFrom(byteArrayInt)

        BlockingQueue<FetchedDataChunk> queue = Queues.newLinkedBlockingQueue()
        pushToStream("testMessage", queue)

        kafkaStream = createMockStream(queue)

        SepMessage sepMessage = PowerMockito.mock(SepMessage.class)
        PowerMockito.when(sepMessage.getRow()).thenReturn(byteString)
        PowerMockito.when(sepMessage.getValue()).thenReturn(byteStringInt)

        SerDe mockedSerde = Stub() {
            execute(_) >> sepMessage
        }
        SerDe.DESERIALZIER = mockedSerde

        kafkaVerifyRunner = new KafkaVerifyRunner(kafkaStream, kafkaAllVersionsMap, runFlag, partitions)
    }

    @Test
    def "consumer no longer usable and throws ConsumerTimeoutException"() {
        // consumer will not accept data after an iteration here and thus throws ConsumerTimeoutException as the timeout is low '100 ms' here
        // To wait indefinitely - timeout can be set to '-1'
        // '-1' case can't be tested here because run method won't complete if the consumer waits indefinitely
        when:
            kafkaVerifyRunner.run()
        then:
            thrown(ConsumerTimeoutException)
            kafkaAllVersionsMap.size() == 1
    }

    @spock.lang.Ignore
    @Test
    def "consumer no longer usable and doesn't throw ConsumerTimeoutException"() {
        /* TODO
           Need to catch the ConsumerTimeoutException in KafkaVerifyRunner and handle it in a desired way.
           Remove the above Test 'consumer no longer usable and throws ConsumerTimeoutException' once this is fixed in
           KafkaVerifyRunner class
         */
        when:
            kafkaVerifyRunner.run()
        then:
            notThrown(ConsumerTimeoutException)
            kafkaAllVersionsMap.size() == 1
    }

    private KafkaStream<byte[], byte[]> createMockStream (BlockingQueue<FetchedDataChunk> queue) {
        KafkaStream<byte[], byte[]> stream = (KafkaStream<byte[], byte[]>) Mockito.mock(KafkaStream.class)
        ConsumerIterator<byte[], byte[]> it =
                new ConsumerIterator<>(queue, 100, new DefaultDecoder(new VerifiableProperties()), new DefaultDecoder(new VerifiableProperties()), "clientId")
        Mockito.when(stream.iterator()).thenReturn(it)
        return stream
    }

    private void pushToStream (String message, BlockingQueue<FetchedDataChunk> queue) {
        AtomicLong offset = new AtomicLong(1)
        AtomicLong thisOffset = new AtomicLong(offset.incrementAndGet())

        List<Message> seq = new ArrayList<>()
        seq.add(new Message(message.getBytes(Charsets.UTF_8)))
        ByteBufferMessageSet messageSet = new ByteBufferMessageSet(NoCompressionCodec$.MODULE$, offset, JavaConversions.asScalaBuffer(seq))

        FetchedDataChunk chunk = new FetchedDataChunk(messageSet, new PartitionTopicInfo("topic", 1, queue,
                thisOffset, thisOffset, new AtomicInteger(1), "clientId"), thisOffset.get())

        queue.add(chunk)
    }
}