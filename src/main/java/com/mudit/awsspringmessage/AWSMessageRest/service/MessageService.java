package com.mudit.awsspringmessage.AWSMessageRest.service;

import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DetectDominantLanguageRequest;
import software.amazon.awssdk.services.comprehend.model.DetectDominantLanguageResponse;
import software.amazon.awssdk.services.comprehend.model.DominantLanguage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MessageService {

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

    private SqsClient getClient() {
        return SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(new AwsCredentialsProvider() {
                    @Override
                    public AwsCredentials resolveCredentials() {
                        return awsCredentials();
                    }
                })
                .build();
    }

    // Get a Comprehend client.
    private ComprehendClient getComClient() {

        return ComprehendClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(new AwsCredentialsProvider() {
                    @Override
                    public AwsCredentials resolveCredentials() {
                        return awsCredentials();
                    }
                })
                .build();
    }


    public void processMessage(MessageData msg) {
        SqsClient sqsClient = getClient();

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

            // We will get the language code for the incoming message.
            ComprehendClient comClient = getComClient();

            // Specify the Langauge code of the incoming message.
            String lanCode = "";
            DetectDominantLanguageRequest request = DetectDominantLanguageRequest.builder()
                    .text(msg.getBody())
                    .build();

            DetectDominantLanguageResponse resp = comClient.detectDominantLanguage(request);
            List<DominantLanguage> allLanList = resp.languages();
            for (DominantLanguage lang : allLanList) {
                System.out.println("Language is " + lang.languageCode());
                lanCode = lang.languageCode();
            }

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageAttributes(myMap)
                    .messageGroupId("GroupA_" + lanCode)
                    .messageDeduplicationId(msg.getId())
                    .messageBody(msg.getBody())
                    .build();

            sqsClient.sendMessage(sendMsgRequest);

        } catch (SqsException e) {
            e.getStackTrace();
        }
    }

    public List<MessageData> getMessages() {
        List<String> attr = new ArrayList<>();
        attr.add("Name");
        SqsClient sqsClient = getClient();

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
            MessageData myMessage;
            List<MessageData> allMessages = new ArrayList<>();

            // Push the messages to a list.
            for (Message m : messages) {
                myMessage = new MessageData();
                myMessage.setBody(m.body());
                myMessage.setId(m.messageId());

                Map<String, MessageAttributeValue> map = m.messageAttributes();
                MessageAttributeValue val = map.get("Name");
                myMessage.setName(val.stringValue());
                allMessages.add(myMessage);
            }

            return allMessages;

        } catch (SqsException e) {
            e.getStackTrace();
        }
        return null;
    }

    public void purgeMyQueue() {
        SqsClient sqsClient = getClient();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        PurgeQueueRequest queueRequest = PurgeQueueRequest.builder()
                .queueUrl(sqsClient.getQueueUrl(getQueueRequest).queueUrl())
                .build();

        sqsClient.purgeQueue(queueRequest);
    }
}
