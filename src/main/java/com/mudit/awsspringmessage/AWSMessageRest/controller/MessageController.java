package com.mudit.awsspringmessage.AWSMessageRest.controller;

import com.mudit.awsspringmessage.AWSMessageRest.Dto.MessageDto;
import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import com.mudit.awsspringmessage.AWSMessageRest.service.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/chat")
public class MessageController {

    @Autowired
    MessageService messageService;

    @PostMapping("/add")
    List<MessageData> addItems(@RequestBody MessageDto messageDto) {
        String user = messageDto.getUser();
        String message = messageDto.getMessage();

        // Generate the ID.
        UUID uuid = UUID.randomUUID();
        String msgId = uuid.toString();

        MessageData messageOb = new MessageData();
        messageOb.setId(msgId);
        messageOb.setName(user);
        messageOb.setBody(message);
        messageService.processMessage(messageOb);
        return messageService.getMessages();
    }

    // Purge the queue.
    @GetMapping("/purge")
    String purgeMessages() {
        messageService.purgeMyQueue();
        return "Queue is purged";
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
        List<MessageData> data = messageService.getMessages();
        return data;
    }
}
