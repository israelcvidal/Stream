package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class TestProducer {
//    public static void main(String[] args) {
//        long events = Long.parseLong(args[0]);
//        Random rnd = new Random();
//        String path = "/home/local/LSBD/israel.vidal/Área de Trabalho/test.txt";
// 
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092,localhost:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "test.SimplePartitioner");
//        props.put("request.required.acks", "1");
// 
//        ProducerConfig config = new ProducerConfig(props);
// 
//        Producer<String, String> producer = new Producer<String, String>(config);
// 
//        for (long nEvents = 0; nEvents < events; nEvents++) { 
//               long runtime = new Date().getTime();  
//               String ip = "192.168.2." + rnd.nextInt(255); 
//               String msg = runtime + ",www.example.com," + ip; 
//               KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
//               producer.send(data);
//               try {
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//        producer.close();
//    }
	
	public static void main(String[] args) throws InterruptedException {
        String filePath = "/home/local/LSBD/israel.vidal/Área de Trabalho/test.txt";
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9092 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "test.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        try{
        	BufferedReader reader = new BufferedReader(new FileReader(filePath));
        	List<String> records = new ArrayList<String>();	
        	String line;
        	while ((line = reader.readLine()) != null){
        		records.add(line);
        	}
        	reader.close();
        	
            for (String msg : records) {
                		   
            	KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", msg);
            	producer.send(data);
//    			Thread.sleep(1000);
    		}
        	
        } catch (IOException e){
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        }
        producer.close();
    }
}