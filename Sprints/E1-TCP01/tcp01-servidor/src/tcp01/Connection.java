package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream in;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            in = new ObjectInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());

            // Inicia a thread automaticamente assim que o objeto é criado
            this.start();

        } catch (IOException e) {
            System.out.println("Connection constructor IO: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            // Ler o objeto Person enviado pelo cliente
            Person person = (Person) in.readObject();

            // Obter a localidade através da referência Place
            String locality = person.getPlace().getLocality();

            // Responder ao cliente com a localidade em vez do nome
            out.writeUTF(locality);
            out.flush();

        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("Class Not Found: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("close: " + e.getMessage());
            }
        }
    }
}