package dslabs.primarybackup;

import dslabs.framework.Message;
import lombok.Data;

/* -------------------------------------------------------------------------
    ViewServer Messages
   -----------------------------------------------------------------------*/
@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    private final View view;
}

/* -------------------------------------------------------------------------
    Primary-Backup Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    // TODO: client request ...
}

@Data
class Reply implements Message {
    //TODO: server response ...
}

@Data
class BackupRequest implements Message {
    // TODO: primary send backup request to backup ...
}

@Data
class BackupReply implements Message {
    // TODO: backup send reply to primary ...
}

@Data
class SyncRequest implements Message {
    // TODO: primary sync up with backup ...
}

@Data
class SyncReply implements Message {
    // TODO: backup send reply to primary ...
}

// TODO: add more messages here ...
