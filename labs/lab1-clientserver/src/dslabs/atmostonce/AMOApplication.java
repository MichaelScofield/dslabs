package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    private final ConcurrentMap<Address, AMOResult> clientLatestResults = new ConcurrentHashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;
        Address clientAddress = amoCommand.clientAddress();
        int sequenceNum = amoCommand.sequenceNum();
        if (alreadyExecuted(amoCommand)) {
            // We cannot store every executed command due to GC pressure,
            // so we reply with the most recently executed command
            // (might not match the client's retry request),
            // and hope the client can deal with it.
            return clientLatestResults.get(clientAddress);
        }
        Result result = application.execute(amoCommand.command());
        AMOResult amoResult = new AMOResult(result, sequenceNum);
        clientLatestResults.put(clientAddress, amoResult);
        return amoResult;
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(AMOCommand amoCommand) {
        AMOResult result = clientLatestResults.get(amoCommand.clientAddress());
        return result != null && result.sequenceNum() >= amoCommand.sequenceNum();
    }
}
