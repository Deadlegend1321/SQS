package com.mudit.awsspringmessage.AWSMessageRest.Repository;

import com.mudit.awsspringmessage.AWSMessageRest.Model.MessageData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MessageRepo extends JpaRepository<MessageData, Long> {

    MessageData save(MessageData messageData);

}
