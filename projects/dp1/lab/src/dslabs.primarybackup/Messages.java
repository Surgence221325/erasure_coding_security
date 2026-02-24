package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import dslabs.framework.Result;
import lombok.Data;
import lombok.NonNull;

@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    @NonNull private final View view;
}

/* ---------------- PB messages ---------------- */

@Data
class Request implements Message {
    @NonNull private final AMOCommand command;
}

@Data
class Reply implements Message {
    @NonNull private final Result result;
}

@Data
class ForwardRequest implements Message {
    @NonNull private final AMOCommand command;
    @NonNull private final View view;
}

@Data
class ForwardReply implements Message {
    @NonNull private final PBServer.PBResult result;
    private final AMOResult amoResult; // nullable: only set on BackupSuccess
    @NonNull private final View view;
}

@Data
class StateTransfer implements Message {
    @NonNull private final AMOApplication<?> app;
    @NonNull private final View view;
}

@Data
class StateTransferAck implements Message {
    @NonNull private final View view;
}