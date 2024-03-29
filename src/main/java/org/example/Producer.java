package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

public class Producer {

    private final static String QUEUE_NAME = "simple_queue";
    private final static String QUEUE_NAME2 = "simple_queue2";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Read tasks from the JSON file as a generic List<Map<String, Object>>
        ObjectMapper objectMapper = new ObjectMapper();
        File jsonFile = new File("src/main/java/org/example/tasks.json");
        List<Map<String, Object>> tasks = objectMapper.readValue(jsonFile, new TypeReference<List<Map<String, Object>>>() {});

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            for (Map<String, Object> task : tasks) {
                // Convert task to JSON string
                String taskJson = convertTaskToJson(task);

                // Send the JSON string to the queue
                if(taskJson.contains("consumer1")) {
                    channel.basicPublish("", QUEUE_NAME, null, taskJson.getBytes());
                    System.out.println(" [x] Sent '" + taskJson + "' to consumer1");
                    Thread.sleep(1000); // Sleep for 1 second between messages
                }
                else if (taskJson.contains("consumer2")){
                    channel.basicPublish("", QUEUE_NAME2, null, taskJson.getBytes());
                    System.out.println(" [x] Sent '" + taskJson + "' to consumer2");
                    Thread.sleep(1000); // Sleep for 1 second between messages
                }
            }
        }
    }

    private static String convertTaskToJson(Map<String, Object> task) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(task);
    }
}
