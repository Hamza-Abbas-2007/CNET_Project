import java.io.IOException;
import java.net.*;
import java.util.Scanner;

public class UDPReceiver {


    private static final int PORT = 2189;
    private static final int SUM = 30;
    private static final String LEADER_ID = "19002189";
    private static final String EXPECTED_HELLO = "HELLO-GIU-" + LEADER_ID + "-" + SUM;
    private static final String OK_REPLY = "OK-GIU-" + LEADER_ID + "-" + SUM;
    private volatile int lastAckReceived = -1;

    private DatagramSocket socket;
    private InetAddress clientAddress;
    private int clientPort;
    private int sequenceNumber = 1;
    private final Scanner keyboard = new Scanner(System.in);
    private volatile boolean connected = true; // volatile so both threads see updates

    public UDPReceiver() throws IOException {
        socket = new DatagramSocket(PORT);
        System.out.println("[SERVER] Listening on port " + PORT);
        System.out.println("[SERVER] Waiting for connection...");

        if (doHandshake()) {
            chat();
        } else {
            System.out.println("[SERVER] Handshake failed. Closing.");
            socket.close();
        }
    }


    private boolean doHandshake() throws IOException {
        String received = receiveRaw();

        System.out.println("[SERVER] Received: " + received);

        if (received.equals(EXPECTED_HELLO)) {
            sendRaw(OK_REPLY, 0, clientAddress, clientPort);
            System.out.println("[SERVER] Handshake complete.");
            System.out.println("[SERVER] Connection established!");
            return true;
        } else {
            sendRaw("REJECTED", 0, clientAddress, clientPort);
            System.out.println("[SERVER] Handshake rejected.");
            return false;
        }
    }


    private void chat() {

        // Thread 1 — receives incoming messages and sends ACKs
        Thread receiverThread = new Thread(() -> {
            while (connected) {
                try {
                    String message = receiveRaw();
                    handleIncomingPacket(message);
                } catch (IOException e) {
                    if (connected) {
                        System.out.println("[SERVER] Receiver error: " + e.getMessage());
                    }
                    break;
                }
            }
        });

        receiverThread.start();


        while (connected) {
            String message = keyboard.nextLine();

            if (!connected) break;

            try {
                if (message.equals("EXIT")) {
                    sendRaw("EXIT", 0, clientAddress, clientPort);
                    System.out.println("[SERVER] You closed the connection.");
                    connected = false;
                    socket.close();
                    break;
                }

                sendWithRetry(message, sequenceNumber++);

            } catch (IOException e) {
                if (connected) {
                    System.out.println("[SERVER] Send error: " + e.getMessage());
                }
                break;
            }
        }
    }


    private void sendWithRetry(String message, int seq) throws IOException {
        byte[] packetBytes = buildPacket(message, seq);
        DatagramPacket packet = new DatagramPacket(
                packetBytes, packetBytes.length, clientAddress, clientPort
        );

        for (int attempt = 0; attempt < 2; attempt++) {
            socket.send(packet);
            GIULogger.log("SEND", packetBytes, System.currentTimeMillis());
            System.out.println("[SENT | Seq: " + seq + "] " + message);

            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < 3000) {
                if (lastAckReceived == seq) {
                    return;
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (attempt == 0) {
                System.out.println("[TIMEOUT] Retrying...");
            } else {
                System.out.println("[CONNECTION LOST]");
                connected = false;
                socket.close();
            }
        }
    }


    private void sendAck(int seq) throws IOException {
        String ackMsg = "ACK:" + seq;
        byte[] payload = ackMsg.getBytes();
        byte[] buffer = new byte[2 + payload.length];
        buffer[0] = 0;
        buffer[1] = 0;
        System.arraycopy(payload, 0, buffer, 2, payload.length);

        DatagramPacket ackPacket = new DatagramPacket(
                buffer, buffer.length, clientAddress, clientPort
        );
        socket.send(ackPacket);
        GIULogger.log("SEND", buffer, System.currentTimeMillis());
        System.out.println("[ACK SENT] ACK:" + seq);
    }


    private byte[] buildPacket(String message, int seq) {
        byte[] payload = message.getBytes();
        byte[] buffer = new byte[2 + payload.length];
        buffer[0] = (byte) (seq >> 8);
        buffer[1] = (byte) (seq & 0xFF);
        System.arraycopy(payload, 0, buffer, 2, payload.length);
        return buffer;
    }


    private int lastReceivedSeq = 0;


    private String receiveRaw() throws IOException {
        byte[] buffer = new byte[1500];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.receive(packet);

        GIULogger.log("RECV", buffer, System.currentTimeMillis());

        clientAddress = packet.getAddress();
        clientPort = packet.getPort();

        lastReceivedSeq = (Byte.toUnsignedInt(buffer[0]) << 8) | Byte.toUnsignedInt(buffer[1]);
        return new String(buffer, 2, packet.getLength() - 2).trim();
    }


    private void sendRaw(String message, int seq, InetAddress address, int port)
            throws IOException {
        byte[] packetBytes = buildPacket(message, seq);
        DatagramPacket packet = new DatagramPacket(
                packetBytes, packetBytes.length, address, port
        );
        socket.send(packet);
        GIULogger.log("SEND", packetBytes, System.currentTimeMillis());
        System.out.println("[SENT | Seq: " + seq + "] " + message);
    }

    private void handleIncomingPacket(String message) throws IOException {
        if (message.equals("EXIT")) {
            System.out.println("[Connection closed by remote.]");
            connected = false;
            socket.close();
            return;
        }

        if (message.startsWith("ACK:")) {
            lastAckReceived = Integer.parseInt(message.substring(4));
            return;
        }

        System.out.println("[RECEIVED] " + message);
        sendAck(lastReceivedSeq);
    }

    public static void main(String[] args) {
        try {
            new UDPReceiver();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}