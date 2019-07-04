/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.learnkafka.mqttdemo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

/**
 *
 * @author bbiadmin
 */
public class MqttExample {

    private static String LANDING_PATH = "E://";
    private static String topic = "/iot/data/logs";
    private static String host = "tcp://localhost:1883";
    private static String FILE_NAME = "sample2-mqtt-log.log";

    public static void main(String[] args) {
        try {
            System.out.println("Starting connection");
            MqttClient client = new MqttClient(host, MqttClient.generateClientId());
            client.connect();
            System.out.println("Connection established");
            MqttMessage message = new MqttMessage();
            FileInputStream fstream = new FileInputStream(LANDING_PATH + FILE_NAME);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            JSONObject headerData = new JSONObject();
            headerData.put("dataType", 1);
            headerData.put("fileName", FILE_NAME);
            message.setPayload(headerData.toString().getBytes());
            // Publish to the topic
            client.publish(topic, message);
            int lineCount = 0;
            while ((strLine = br.readLine()) != null) {
                // Print the content on the console
                JSONObject fileLine = new JSONObject();
                fileLine.put("dataType", 2);
                fileLine.put("fileName", FILE_NAME);
                fileLine.put("data", strLine);
                System.out.println("Publishing - " + strLine);
                // Set the payload
                message.setPayload(fileLine.toString().getBytes());
                // Publish to the topic
                client.publish(topic, message);
                System.out.println("Line Count = " + (++lineCount));
            }

            JSONObject footerData = new JSONObject();
            footerData.put("dataType", 3);
            footerData.put("eol", true);
            message.setPayload(footerData.toString().getBytes());
            // Publish to the topic
            client.publish(topic, message);
            //Close the input stream
            fstream.close();
            client.disconnect();
        } catch (Exception e) {
            System.out.println("Exception while sending message");
        }
    }

    public static void readFile() {
        // Open the file
        try {
            FileInputStream fstream = new FileInputStream(LANDING_PATH + FILE_NAME);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;

            //Read File Line By Line
            while ((strLine = br.readLine()) != null) {
                // Print the content on the console
                System.out.println("I want to publish - " + strLine);
            }

            //Close the input stream
            fstream.close();
        } catch (Exception e) {
            System.out.println("Exception while reading file " + e);
        }

    }

}
