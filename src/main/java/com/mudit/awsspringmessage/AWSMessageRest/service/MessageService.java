package com.mudit.awsspringmessage.AWSMessageRest.service;

import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import com.mudit.awsspringmessage.AWSMessageRest.Repository.MessageRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
public class MessageService {

    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    @Autowired
    MessageRepo messageRepo;

    final Scanner input = new Scanner(System.in);

    final int consumerCount = 3;


    @Value("${cloud.aws.region.static}")
    private String region;

    @Value("${cloud.aws.credentials.access-key}")
    private String awsAccessKey;

    @Value("${cloud.aws.credentials.secret-key}")
    private String awsSecretKey;

    private final String queueName = "Message.fifo";

    private AwsCredentials awsCredentials(){
        AwsCredentials credentials = new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return awsAccessKey;
            }

            @Override
            public String secretAccessKey() {
                return awsSecretKey;
            }
        };
        return credentials;
    }

    final SqsClient sqsClient = SqsClient.builder()
            .region(Region.AP_SOUTH_1)
            .credentialsProvider(new AwsCredentialsProvider() {
                @Override
                public AwsCredentials resolveCredentials() {
                    return awsCredentials();
                }
            })
            .build();

    public void sendMessage(MessageData msg) {

        try {
            MessageAttributeValue attributeValue = MessageAttributeValue.builder()
                    .stringValue(msg.getName())
                    .dataType("String")
                    .build();

            Map<String, MessageAttributeValue> myMap = new HashMap<>();
            myMap.put("Name", attributeValue);
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageAttributes(myMap)
                    .messageGroupId("GroupA_" + "foo")
                    .messageDeduplicationId(msg.getId())
                    .messageBody(msg.getBody())
                    .build();

            sqsClient.sendMessage(sendMsgRequest);
            LOGGER.log( Level.INFO, "Sending message in the queue");

        } catch (SqsException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.getStackTrace();
        }
    }

    public void processMessage() {
        try{
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            Consumer consumer = new Consumer(sqsClient, queueUrl, messageRepo);
            ExecutorService executorService = Executors.newFixedThreadPool(consumerCount);
            executorService.execute(consumer);
            /*final Thread[] threads = new Thread[consumerCount];
            for (int i = 0; i < consumerCount; i++) {
                threads[i] = new Thread(consumer);
                threads[i].start();
            }*/

        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.getStackTrace();
        }
    }

    public List<Message> getMessages(){
        List<String> attr = new ArrayList<>();
        attr.add("Name");
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .messageAttributeNames(attr)
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            return messages;

        } catch (SqsException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.getStackTrace();
        }
        return null;
    }

    public List<MessageData> getAllMessages() {

        try {

            List<Message> messages = getMessages();
            MessageData myMessage;
            List<MessageData> allMessages = new ArrayList<>();

            // Push the messages to a list.
            for (Message m : messages) {
                myMessage = new MessageData();
                myMessage.setBody(m.body());
                myMessage.setId(m.messageId());

                Map<String, MessageAttributeValue> map = m.messageAttributes();
                MessageAttributeValue val = map.get("Name");
                if(val != null){
                    myMessage.setName(val.stringValue());
                }
                allMessages.add(myMessage);
            }
            LOGGER.log( Level.INFO, "Receiving messages from the queue");

            return allMessages;

        } catch (SqsException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.getStackTrace();
        }
        return null;
    }

    public void deleteMessage(String id){
        try{
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            List<Message> messages = getMessages();
            for (Message m : messages) {
                if(m.messageId().equals(id)){
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest
                            .builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqsClient.deleteMessage(deleteMessageRequest);
                    LOGGER.log( Level.WARNING, "Deleting message from the queue", m);
                }
            }
        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.getStackTrace();
        }
    }
}
