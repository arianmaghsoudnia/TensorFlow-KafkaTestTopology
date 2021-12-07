# TensorFlow-KafkaTestTopology

This is a minimal repository that reproduces Tensorflow java 0.4.0 compatibility issue with KafkaStream's Ktable.

After starting a KafkaStreams topology that contains KTable methods, Tensorflow waits indefinitely at  ```org.tensorflow.internal.c_api.global.tensorflow.TF_NewGraph```.

One possible cause could be Ktable using RocksDB under the hood making native C calls that conflicts with Tensorflow's native calls.

To reproduce this issue please use the following test units.

## Test Units

./app/src/test/java/com/streamer/TestTFKafka.java

Contains all the test units to reproduce the problem. 
TF functionality is tested using two Kafka topologies.

### testTFWithSimpleTopology

That takes one input streams and forwards it to an output stream. To get a better idea of how it works you can run testSimpleForwarder unit test.
If you run the testTFWithSimpleTopology, test passes, succussfully creating an empty graph.

### testTFWithKTable

That takes an  input streams, and a coefficients stream, does a KTable aggregation that multiplies the input stream by the values of coefficients stream and forwards it to an output stream. To get a better idea of how it works you can run testForwarderWithKTable unit test.
If you run the testTFWithKTable, test stucks in the thread which calls internal c_api to create a new graph.