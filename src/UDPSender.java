import java.io.IOException;
import java.net.*;
import java.util.Scanner;

public class UDPSender {


    private static final int PORT = 2189;
    private static final int SUM = 30;
    private static final String LEADER_ID = "19002189";
    private static final String HELLO_MSG = "HELLO-GIU-" + LEADER_ID + "-" + SUM;
    private static final String EXPECTED_OK = "OK-GIU-" + LEADER_ID + "-" + SUM;
    private static final String SERVER_IP = "172.22.184.36";
    private volatile int lastAckReceived = -1;

    private DatagramSocket socket;
    private InetAddress serverAddress;
    private int sequenceNumber = 1;
    private final Scanner keyboard = new Scanner(System.in);
    private volatile boolean connected = true;

    public UDPSender() throws IOException {


        System.out.println("[CLIENT] Type CONNECT to open a connection.");
        String input = keyboard.nextLine();

        if (!input.equals("CONNECT")) {
            System.out.println("[CLIENT] Type CONNECT to begin.");
            return;
        }


        socket = new DatagramSocket();
        serverAddress = InetAddress.getByName(SERVER_IP);

        if (doHandshake()) {
            chat();
        } else {
            System.out.println("[CLIENT] Handshake failed. Closing.");
            socket.close();
        }
    }


    private boolean doHandshake() throws IOException {
        sendRaw(HELLO_MSG, 0);
        System.out.println("[CLIENT] HELLO sent: " + HELLO_MSG);

        String response = receiveRaw();
        System.out.println("[CLIENT] Server replied: " + response);

        if (response.equals(EXPECTED_OK)) {
            System.out.println("[CLIENT] Handshake complete.");
            System.out.println("[CLIENT] Connected! Type messages or EXIT.");
            return true;
        } else if (response.equals("REJECTED")) {
            System.out.println("[CLIENT] Server rejected the connection.");
            return false;
        } else {
            System.out.println("[CLIENT] Unexpected response: " + response);
            return false;
        }
    }


    private void chat() {


        Thread receiverThread = new Thread(() -> {
            while (connected) {
                try {
                    String message = receiveRaw();
                    handleIncomingPacket(message);
                } catch (IOException e) {
                    if (connected) {
                        System.out.println("[CLIENT] Receiver error: " + e.getMessage());
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
                    sendRaw("EXIT", 0);
                    System.out.println("[CLIENT] You closed the connection.");
                    connected = false;
                    socket.close();
                    break;
                }

                sendWithRetry(message, sequenceNumber++);

            } catch (IOException e) {
                if (connected) {
                    System.out.println("[CLIENT] Send error: " + e.getMessage());
                }
                break;
            }
        }
    }


    private void sendWithRetry(String message, int seq) throws IOException {
        byte[] packetBytes = buildPacket(message, seq);
        DatagramPacket packet = new DatagramPacket(
                packetBytes, packetBytes.length, serverAddress, PORT
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
                buffer, buffer.length, serverAddress, PORT
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

        lastReceivedSeq = (Byte.toUnsignedInt(buffer[0]) << 8) | Byte.toUnsignedInt(buffer[1]);
        return new String(buffer, 2, packet.getLength() - 2).trim();
    }

    // ── Raw send ──────────────────────────────────────────
    private void sendRaw(String message, int seq) throws IOException {
        byte[] packetBytes = buildPacket(message, seq);
        DatagramPacket packet = new DatagramPacket(
                packetBytes, packetBytes.length, serverAddress, PORT
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
            System.out.println("[ACK RECEIVED] ACK:" + lastAckReceived);
            return;
        }

        System.out.println("[RECEIVED] " + message);
        sendAck(lastReceivedSeq);
    }

    public static void main(String[] args) {
        try {
            new UDPSender();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}