// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Process-wide counters and histograms exported on /metrics.
#pragma once

#include "common/metrics.h"

namespace tq::broker {

struct BrokerMetrics {
    Counter& messagesPublished;
    Counter& messagesDelivered;
    Counter& messagesAcknowledged;
    Counter& messagesRedelivered;
    Counter& messagesReturned;
    Counter& messagesConfirmed;
    Counter& messagesUnroutable;
    Counter& connectionsOpened;
    Counter& connectionsClosed;
    Counter& channelsOpened;
    Counter& authFailures;
    Histogram& storeSyncSeconds;

    static BrokerMetrics& get();
};

}  // namespace tq::broker
