package com.mudit.awsspringmessage.AWSMessageRest.controller;

import com.mudit.awsspringmessage.AWSMessageRest.Dto.MessageDto;
import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import com.mudit.awsspringmessage.AWSMessageRest.service.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/chat")
public class MessageController {

    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    @Autowired
    MessageService messageService;


    @PostConstruct
    public void initialize() {
        // Code to execute when the controller is initialized
        // For example, starting a background task or initializing resources
        processMessages();
        LOGGER.log( Level.INFO, "Starting processMessage() function");
    }

    @PostMapping("/add")
    List<MessageData> addItems(@RequestBody MessageDto messageDto) {
        String user = messageDto.getUser();
        String message = messageDto.getMessage();

        // Generate the ID.
        UUID uuid = UUID.randomUUID();
        String msgId = uuid.toString();

        MessageData messageOb = new MessageData();
        messageOb.setId(msgId);
        if (user == null) {
            messageOb.setName("");
        } else {
            messageOb.setName(user);
        }
        messageOb.setBody(message);
        messageService.sendMessage(messageOb);
        return messageService.getAllMessages();
    }

    // Process the message.
    @GetMapping("/process")
    String processMessages() {
        messageService.processMessage();
        return "Message is processed and deleted";
    }

    // Delete a particular message.
    @GetMapping("/delete/{id}")
    String deleteMessage(@PathVariable("id") String id) {
        messageService.deleteMessage(id);
        return "Message is deleted";
    }

    // Get messages from the FIFO queue.
    @GetMapping("/msgs")
    List<MessageData> getItems() {
        List<MessageData> data = messageService.getAllMessages();
        return data;
    }
}
