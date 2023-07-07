package com.songkhlaf.acdm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.google.gson.Gson;

public class ACDM_AIRPORTINFO {
	static String File_path;
	static String File_name;
	static String absolute_file;

	public static void main(String[] args) {
        // ------------------------------------------------------------------------------------------------------------------------------------------------------
        Properties prop = new Properties();
        File fi = new File("./conf/app.properties");
        String fullName = fi.getAbsolutePath();
        try {
            prop.load(new FileInputStream(fullName));
            File_path = prop.getProperty("File_Path");
            File_name = prop.getProperty("ACDM_AIRPORTINFO_Data_name");

            File f = new File(File_path);
            String[] pathnames = f.list();
            for (int i = 0; i < pathnames.length; i++) {
                if (pathnames[i].contains(File_name))
                    absolute_file = File_path + "/" + pathnames[i];
            }
            
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------
        List<String> content = null;
        try {
            content = Files.readAllLines(Paths.get(absolute_file), StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("ERROR, read text file.");
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------
        Gson gson = new Gson();
        int linenumber = 0;
        for (String one_data : content) {
        	
        	linenumber++;
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("ACDM_AIRPORTINFO");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
                DataObject dataObject = new DataObject(one_data,File_name,linenumber);
                String jsonText = gson.toJson(dataObject);
                System.out.println(jsonText);
                TextMessage message = session.createTextMessage(jsonText);

                // Tell the producer to send the message
                System.out.println("Sent message");
                producer.send(message);

                // Clean up
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        System.out.println("End.");
        // ------------------------------------------------------------------------------------------------------------------------------------------------------
    }

	private static class DataObject {
		private String linedata;
		private String filename;
		private int linenumber;

		public DataObject(String linedata, String filename, int linenumber) {
			this.linedata = linedata;
			this.filename = filename;
			this.linenumber = linenumber;
		}
	}
}
