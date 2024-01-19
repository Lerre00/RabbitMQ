/*
package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.Scanner;

public class Consumer2 {
    private final static String QUEUE_NAME2 = "simple_queue2";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            //channel.queuePurge(QUEUE_NAME);

            channel.queueDeclare(QUEUE_NAME2, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To stop, type 'z' and press Enter.");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                ObjectMapper objectMapper = new ObjectMapper();
                // Deserialize the JSON message into a Map
                Map<String, Object> taskMap = objectMapper.readValue(message, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
                });

                String consumer = (String) taskMap.get("Consumer");
                String taskID = (String) taskMap.get("Task_ID");
                System.out.println(consumer);
                //System.out.println(" [x] Received '" + message + "'");

                    System.out.println(" [x] Received '" + taskID + "'");



                    // Extract relevant information based on the "Command" field
                    String command = (String) taskMap.get("Command");

                    // Call the appropriate method based on the "Command"
                    if (command.equals("CopyFiles")) {
                        copyFiles(taskMap);
                    }
                    else if (command.equals("DeleteFile")) {
                        deleteFile(taskMap);
                    }
                    else if (command.equals("CreateFile")) {
                        createFile(taskMap);
                    }

            };

            Scanner scanner = new Scanner(System.in);

            // Continuous message consumption loop
            while (true) {
                channel.basicConsume(QUEUE_NAME2, true, deliverCallback, consumerTag -> { });
                // Check for user input to stop the consumer
                if (scanner.hasNextLine()) {
                    String userInput = scanner.nextLine();
                    if ("z".equalsIgnoreCase(userInput.trim())) {
                        System.out.println("Stopping the consumer. Goodbye!");
                        break;
                    }
                }
            }
        }
    }
    private static void copyFiles(Map<String, Object> taskMap) {
        // Extract relevant information and perform the CopyFiles operation
        String inputPath = (String) taskMap.get("Input_path");
        String inputFolder = (String) taskMap.get("Input_folder");
        String inputFiles = (String) taskMap.get("Input_files");
        String outputPath = (String) taskMap.get("Output_path");
        String outputFolder = (String) taskMap.get("Output_folder");
        String outputFiles = (String) taskMap.get("Output_files");

        // Build full input and output paths
        Path sourcePath = Paths.get(inputPath, inputFolder, inputFiles);
        Path destinationPath = Paths.get(outputPath, outputFolder, outputFiles);

        try {
            // Create necessary directories if they don't exist
            Files.createDirectories(destinationPath.getParent());

            // Perform the file copy
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);

            System.out.println("File copied successfully:");
            System.out.println("Source: " + sourcePath);
            System.out.println("Destination: " + destinationPath);

        } catch (IOException e) {
            System.err.println("Error copying the file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void deleteFile(Map<String, Object> taskMap) {
        // Extract relevant information and perform the DeleteFile operation
        String inputPath = (String) taskMap.get("Input_path");
        String inputFolder = (String) taskMap.get("Input_folder");
        String inputFile = (String) taskMap.get("Input_files");

        // Build full input path
        Path filePath = Paths.get(inputPath, inputFolder, inputFile);

        try {
            // Perform the file deletion
            Files.delete(filePath);

            System.out.println("File deleted successfully:");
            System.out.println("File: " + filePath);

        } catch (NoSuchFileException e) {
            System.err.println("File not found: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error deleting the file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void createFile(Map<String, Object> taskMap) {
        // Extract relevant information and perform the DeleteFile operation
        String inputPath = (String) taskMap.get("Input_path");
        String inputFolder = (String) taskMap.get("Input_folder");
        String inputFile = (String) taskMap.get("Input_files");

        // Build full input path
        Path filePath = Paths.get(inputPath, inputFolder, inputFile);

        try {
            // Create necessary directories if they don't exist
            Files.createDirectories(filePath.getParent());

            // Create the file
            Files.createFile(filePath);

            System.out.println("File created successfully:");
            System.out.println("File: " + filePath);

        } catch (IOException e) {
            System.err.println("Error creating the file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

 */