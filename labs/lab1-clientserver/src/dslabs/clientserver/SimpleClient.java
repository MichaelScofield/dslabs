package dslabs.clientserver;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {

    private volatile int sequence = 1;

    private final Address serverAddress;

    private volatile Request outstandingRequest;

    private volatile Reply reply;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleClient(Address address, Address serverAddress) {
        super(address);
        this.serverAddress = serverAddress;
    }

    @Override
    public synchronized void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        Request request = new Request(new AMOCommand(address(), command, sequence));
        send(request, serverAddress);
        outstandingRequest = request;
        set(new ClientTimer(request), ClientTimer.CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        return reply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        while (reply == null) {
            wait();
        }
        Result result = reply.result().result();
        reply = null;
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        if (outstandingRequest == null) {
            return;
        }
        if (m == null || m.result() == null ||
                m.result().sequenceNum() != outstandingRequest.command().sequenceNum()) {
            return;
        }
        reply = m;
        outstandingRequest = null;
        sequence = m.result().sequenceNum() + 1;
        notifyAll();
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (outstandingRequest != null &&
                outstandingRequest.command().sequenceNum() == t.request().command().sequenceNum()) {
            send(t.request(), serverAddress);
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }
}
