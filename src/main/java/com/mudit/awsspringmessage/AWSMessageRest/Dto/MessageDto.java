package com.mudit.awsspringmessage.AWSMessageRest.Dto;

public class MessageDto {
    private String message;
    private String user;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }



    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
