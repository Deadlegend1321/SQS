package com.mudit.awsspringmessage.AWSMessageRest.service;

import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import com.mudit.awsspringmessage.AWSMessageRest.Repository.MessageRepo;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class Consumer implements Runnable {
    final SqsClient sqsClient;
    final String queueUrl;
    MessageRepo messageRepo;

    Consumer(SqsClient sqsClient, String queueUrl, MessageRepo messageRepo) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.messageRepo = messageRepo;
    }

    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    @Override
    public void run() {
        try {
            while(true){
                List<String> attr = new ArrayList<>();
                attr.add("Name");
                final ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(1)
                        .messageAttributeNames(attr)
                        .build();
                List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
                LOGGER.log( Level.INFO, "receiving message from queue");

                if (!messages.isEmpty()) {
                    for (Message m : messages) {
                        LOGGER.log( Level.INFO, "processing entries in loop");
                        MessageData messageData = new MessageData();
                        messageData.setName(String.valueOf(m.messageAttributes().get("Name")));
                        messageData.setBody(m.body());
                        messageData.setId(m.messageId());
                        LOGGER.log( Level.INFO, "Saving message in database");
                        messageRepo.save(messageData);
                        LOGGER.log( Level.WARNING, "Deleting message from queue");
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest
                                .builder()
                                .queueUrl(queueUrl)
                                .receiptHandle(m.receiptHandle())
                                .build();
                        sqsClient.deleteMessage(deleteMessageRequest);
                    }
                }
            }
        } catch (SqsException e) {
            LOGGER.log(Level.SEVERE, "Consumer: " + e.getMessage(), e);
            System.exit(1);
        }
    }
}
