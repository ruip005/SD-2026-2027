package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;
            s = new Socket("localhost", serverPort);
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());

            Place place = new Place("1000-001", "Lisboa");

            // Criar o objeto Person usando o novo construtor
            Person person = new Person("Maria Santos", place, 1995);

            // Enviar o objeto para o servidor (usando 'out')
            out.writeObject(person);
            out.flush(); // Garante o envio imediato dos dados

            // Ler a resposta do servidor (usando 'in')
            String response = in.readUTF();
            System.out.println("Resposta do servidor: " + response);

        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}