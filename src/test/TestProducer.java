package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class TestProducer {
	
	public static void main(String[] args) throws InterruptedException {
        String filePath = "/home/local/LSBD/israel.vidal/workspace/Stream/geolocation.csv";
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9092 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "test.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        try{
//        	Lendo o arquivo para usar como simulação de streaming
        	BufferedReader reader = new BufferedReader(new FileReader(filePath));
        	List<String> records = new ArrayList<String>();	
        	String line;
        	
        	while ((line = reader.readLine()) != null){
        		String city = line.split(",")[3];
        		records.add(city);
        	}
        	reader.close();
        	
//        	Simulando produção de localização com os dados do arquivo
            for (String msg : records) {
            	KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", msg);
            	producer.send(data);
    		}
        	
        } catch (IOException e){
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        }
        producer.close();
    }
}