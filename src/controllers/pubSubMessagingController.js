/* Author: Dhrumil Amish Shah (B00857606) */
require('dotenv').config();
const { PubSub, v1 } = require("@google-cloud/pubsub");

const pubSubClient = new PubSub({
    keyFilename: 'csci5410-group5-safedeposit.json'
});
const subClient = new v1.SubscriberClient({
    keyFilename: 'csci5410-group5-safedeposit.json'
});

const createTopic = async (userId, email) => {
    try {
        const topicName = `topic-${userId}`;
        await pubSubClient.createTopic(topicName);
        return {
            message: `Topic ${topicName} created by user with account ${email}.`,
            success: true,
            payload: topicName,
            statusCode: 200,
        };
    } catch (error) {
        console.log(error);
        return {
            message: "Internal Server Error",
            success: false,
            payload: undefined,
            statusCode: 500,
        };
    }
};

const publishMessage = async (topicName, message) => {
    try {
        const messageBuffer = Buffer.from(message);
        const messageId = await pubSubClient.topic(topicName).publishMessage({ data: messageBuffer });
        return {
            message: `Message ${messageId} published by topic ${topicName}.`,
            success: true,
            payload: topicName,
            statusCode: 200,
        };
    } catch (error) {
        console.log(error);
        return {
            message: "Internal Server Error",
            success: false,
            payload: undefined,
            statusCode: 500,
        };
    }
};

const createSubscription = async (topicName, userId) => {
    const subscriptionName = `subscription-${userId}-${topicName}`;
    try {
        await pubSubClient.topic(topicName).createSubscription(subscriptionName);
    } catch (error) {
        // console.log(error);
    }
    return subscriptionName;
};

const pullDelivery = async (topicName, userId) => {
    try {
        const subscriptionName = await createSubscription(topicName, userId);
        const formattedSubscription = subClient.subscriptionPath(process.env.FIREBASE_PROJECT_ID, subscriptionName);
        const [response] = await subClient.pull({ subscription: formattedSubscription, maxMessages: 10 });

        const messages = [];
        const ackIds = [];
        for (const msg of response.receivedMessages) {
            console.log(`Received message: ${msg.message.data}`);
            messages.push(Buffer.from(msg.message.data, "base64").toString("utf-8"));
            ackIds.push(msg.ackId);
        }

        if (ackIds.length !== 0) {
            const ackRequest = { subscription: formattedSubscription, ackIds: ackIds, };
            await subClient.acknowledge(ackRequest);
        }

        return {
            message: `Messages retrived by subscription ${subscriptionName}.`,
            success: true,
            payload: messages,
            statusCode: 200,
        };
    } catch (error) {
        console.log(error);
        return {
            message: "Internal Server Error",
            success: false,
            payload: undefined,
            statusCode: 500,
        };
    }
};

module.exports = {
    createTopic,
    publishMessage,
    pullDelivery
};