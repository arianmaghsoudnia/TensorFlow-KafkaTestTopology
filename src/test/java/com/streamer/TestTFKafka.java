package com.streamer;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.jupiter.api.Test;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import org.tensorflow.SavedModelBundle;

public class TestTFKafka {

    private String InputTopicName = "input-topic";
    private String CoefficientsTopicName = "coefficients-topic";
    private String OutputTopicName = "output-topic";

    private String ModelPath = "/home/arian/Blockbax/Projects/tf_java_test/model";

    private TestTopologyExtension<String, Float> testTopology;

    protected TestTopologyExtension<String, Float> getTestTopology() {
        return testTopology;
    }

    public Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamer-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Float().getClass());

        return props;
    }

    protected void produceInput(String key, Float value) {

        getTestTopology()
            .input(InputTopicName)
            .at(System.currentTimeMillis())
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Float())
            .add(key, value);
    }

    protected void produceCoefficients(String key, Float value) {

        getTestTopology()
            .input(CoefficientsTopicName)
            .at(System.currentTimeMillis())
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Float())
            .add(key, value);
    }

    protected ProducerRecord<String, Float> getOneOutput() {
        return getTestTopology()
            .streamOutput(OutputTopicName)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Float())
            .readOneRecord();
    }


    public Topology createSimpleForwarderTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Float> inputKStream = builder.stream(InputTopicName);
        inputKStream.to(OutputTopicName);

        Topology testTopology = builder.build();
        return testTopology;
    }

    public Topology createTopologyWithKTable() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Float> inputKStream = builder.stream(InputTopicName);
        KStream<String,Float> coefficientsStream = builder.stream(CoefficientsTopicName);

        KTable<String,Float> kTable = inputKStream.groupByKey()
            .aggregate(()-> 0f,(k,v,VR)->v, Materialized.as("test-table"));

        KStream<String,Float> outputStream = coefficientsStream.join(kTable,(leftValue,rightValue)-> leftValue*rightValue);

        outputStream.to(OutputTopicName);

        Topology testTopology = builder.build();
        return testTopology;
    }

    @Test
    public void testSimpleForwarder(){
        testTopology = new TestTopologyExtension<>(this::createSimpleForwarderTopology, getProperties());
        testTopology.start();
        produceInput("TestInput",12.3f);
        System.out.print("Latest retreived output is " +  getOneOutput().value() + "\n");
    }

    @Test
    public void testForwarderWithKTable(){
        testTopology = new TestTopologyExtension<>(this::createTopologyWithKTable, getProperties());
        testTopology.start();
        produceInput("TestInput",12.3f);
        produceCoefficients("TestInput",2f);

        System.out.print("Latest retreived output is " +  getOneOutput().value() + "\n");
    }

    @Test void testTFWithSimpleTopology(){
        testTopology = new TestTopologyExtension<>(this::createSimpleForwarderTopology, getProperties());
        testTopology.start();
        SavedModelBundle model = SavedModelBundle.load(ModelPath);
        System.out.print("Model fetched"+model.signatures());
    }

    @Test void testTFWithKTable(){
        testTopology = new TestTopologyExtension<>(this::createTopologyWithKTable, getProperties());
        testTopology.start();
        SavedModelBundle model = SavedModelBundle.load(ModelPath);
        System.out.print("Model fetched"+model.signatures());
    }


}





