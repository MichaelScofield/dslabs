package dslabs.clientserver;

import dslabs.framework.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {

    private static final AtomicInteger SEQUENCE_NUM = new AtomicInteger(0);

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
        Request request = new Request(command, SEQUENCE_NUM.getAndIncrement());
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
        Result result = reply.result();
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
        if (outstandingRequest.sequenceNum() != m.sequenceNum()) {
            return;
        }
        reply = m;
        outstandingRequest = null;
        notifyAll();
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (outstandingRequest != null && outstandingRequest.sequenceNum() == t.request().sequenceNum()) {
            send(t.request(), serverAddress);
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }
}
