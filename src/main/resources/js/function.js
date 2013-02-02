var messageList;
var messageBox;
var name = prompt("Enter your nickname");

function doLongPoll() {
    console.log("Starting long poll!!")
    $.ajax({url: '/comet', data: {id:name}})
        .done(function(message) {
            if(message.length > 0) {
                console.log("Received long poll message: " + message);
                messageList.append('<li>'+message+'</li>');
            }
        })
        .fail(function() {
            console.log("Long Poll Failed");
        })
        .always(doLongPoll);
}

function sendMessage(event) {
    event.preventDefault();
    var message = messageBox.val();
    messageBox.val('');
    $.ajax({url: '/sendMessage', data: {name:name, message:message}})
        .done(function() {
            console.log("sendMessage success");
        });
}

$(document).ready(function() {
    messageList = $('#message-list');
    messageBox = $('#message-box');
    doLongPoll();
    $("#chat-box").on("submit", sendMessage);
})