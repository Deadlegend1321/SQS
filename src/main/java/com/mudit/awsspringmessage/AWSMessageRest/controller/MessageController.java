package com.mudit.awsspringmessage.AWSMessageRest.controller;

import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import com.mudit.awsspringmessage.AWSMessageRest.service.MessageService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
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
    List<MessageData> addItems(HttpServletRequest request, HttpServletResponse response) {
        String user = request.getParameter("user");
        String message = request.getParameter("message");

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
    String purgeMessages(HttpServletRequest request, HttpServletResponse response) {
        messageService.purgeMyQueue();
        return "Queue is purged";
    }

    // Get messages from the FIFO queue.
    @GetMapping("/msgs")
    List<MessageData> getItems(HttpServletRequest request, HttpServletResponse response) {
        List<MessageData> data = messageService.getMessages();
        return data;
    }
}
