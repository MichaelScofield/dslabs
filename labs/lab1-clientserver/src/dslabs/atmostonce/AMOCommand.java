package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
    private final Address clientAddress;
    private final Command command;
    private final int sequenceNum;
}
