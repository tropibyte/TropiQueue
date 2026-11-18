// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/constants.h"

namespace tq::amqp {

std::string_view replyCodeName(uint16_t code) {
    switch (code) {
        case kReplySuccess: return "REPLY_SUCCESS";
        case kContentTooLarge: return "CONTENT_TOO_LARGE";
        case kNoRoute: return "NO_ROUTE";
        case kNoConsumers: return "NO_CONSUMERS";
        case kConnectionForced: return "CONNECTION_FORCED";
        case kInvalidPath: return "INVALID_PATH";
        case kAccessRefused: return "ACCESS_REFUSED";
        case kNotFound: return "NOT_FOUND";
        case kResourceLocked: return "RESOURCE_LOCKED";
        case kPreconditionFailed: return "PRECONDITION_FAILED";
        case kFrameError: return "FRAME_ERROR";
        case kSyntaxError: return "SYNTAX_ERROR";
        case kCommandInvalid: return "COMMAND_INVALID";
        case kChannelError: return "CHANNEL_ERROR";
        case kUnexpectedFrame: return "UNEXPECTED_FRAME";
        case kResourceError: return "RESOURCE_ERROR";
        case kNotAllowed: return "NOT_ALLOWED";
        case kNotImplemented: return "NOT_IMPLEMENTED";
        case kInternalError: return "INTERNAL_ERROR";
        default: return "UNKNOWN";
    }
}

bool isHardError(uint16_t code) {
    switch (code) {
        case kConnectionForced:
        case kInvalidPath:
        case kFrameError:
        case kSyntaxError:
        case kCommandInvalid:
        case kChannelError:
        case kUnexpectedFrame:
        case kResourceError:
        case kNotAllowed:
        case kNotImplemented:
        case kInternalError:
            return true;
        default:
            return false;
    }
}

}  // namespace tq::amqp
