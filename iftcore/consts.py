"""
Messages that can sent from iftd to a transmitter
"""
PROTO_MSG_NONE    = 0      # dummy message to be ignored
PROTO_MSG_INIT    = 1      # do one-time setup for transmission
PROTO_MSG_START   = 2      # begin to transmit
PROTO_MSG_TERM    = 5      # terminate a transmission, even if it isn't finished
PROTO_MSG_END     = 6      # finish the current transmission and stop 
PROTO_MSG_ERROR   = 9      # an recoverable error occured during transmission
PROTO_MSG_ERROR_FATAL = 10 # an irrecoverable error occurred during transmission
PROTO_MSG_USER    = 100    # base key to add to when defining additional messages in subclasses

PROTO_MSG_SETSTATE = PROTO_MSG_USER    # forcibly change protocol state


"""
Internal protocol capabilities
"""
PROTO_DETERMINISTIC_CHUNKING     = "PROTO_DETERMINISTIC_CHUNKING"
PROTO_NONDETERMINISTIC_CHUNKING  = "PROTO_NONDETERMINISTIC_CHUNKING"
PROTO_NO_CHUNKING                = "PROTO_NO_INTERNAL_CHUNKING"

PROTO_USE_DEPRICATED          = "PROTO_USE_DEPRICATED"         # Specify this as a setup attr if the protocol is depricated.


"""
States of transmission an ifttransmit instance can be in
"""
PROTO_STATE_DEAD        = 0   # no transmission, and the thread isn't running
PROTO_STATE_RUNNING     = 2   # transmission is occurring
PROTO_STATE_ENDED       = 3   # no transmission, and there is nothing to do
PROTO_STATE_TERM        = 4   # no transmission, protocol was terminated

"""
Common connection (protocol) attributes
"""
PROTO_PORTNUM        = "PROTO_PORTNUM"


"""
Protocol states
"""
TRANSMIT_STATE_DEAD = 0
TRANSMIT_STATE_CHUNKS = 9
TRANSMIT_STATE_SUCCESS = 10
TRANSMIT_STATE_FAILURE = 11
