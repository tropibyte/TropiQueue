// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/broker_metrics.h"

namespace tq::broker {

BrokerMetrics& BrokerMetrics::get() {
    auto& r = MetricsRegistry::global();
    static BrokerMetrics m{
        r.counter("tropiqueue_messages_published_total", "Messages published by AMQP clients"),
        r.counter("tropiqueue_messages_delivered_total", "Messages delivered to consumers and basic.get"),
        r.counter("tropiqueue_messages_acknowledged_total", "Deliveries acknowledged by clients"),
        r.counter("tropiqueue_messages_redelivered_total", "Deliveries with the redelivered flag set"),
        r.counter("tropiqueue_messages_returned_total", "Mandatory messages returned to publishers"),
        r.counter("tropiqueue_messages_confirmed_total", "Publisher confirms sent"),
        r.counter("tropiqueue_messages_unroutable_total", "Messages that matched no queue"),
        r.counter("tropiqueue_connections_opened_total", "AMQP connections opened"),
        r.counter("tropiqueue_connections_closed_total", "AMQP connections closed"),
        r.counter("tropiqueue_channels_opened_total", "AMQP channels opened"),
        r.counter("tropiqueue_auth_attempts_failed_total", "Failed authentication attempts"),
        r.histogram("tropiqueue_store_sync_seconds", "Duration of message store syncs"),
    };
    return m;
}

}  // namespace tq::broker
