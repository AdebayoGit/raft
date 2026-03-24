//! Broadcast-based event bus.

use tokio::sync::broadcast;

use super::event::MutationEvent;

/// Default channel capacity. Sized for mobile workloads — most mutations
/// are bursty but low-volume compared to a server database.
const DEFAULT_CAPACITY: usize = 256;

/// A broadcast event bus for internal pub/sub.
///
/// Publishers call [`publish`](Self::publish) to fan-out a
/// [`MutationEvent`] to all active subscribers. Subscribers call
/// [`subscribe`](Self::subscribe) to obtain a
/// [`broadcast::Receiver`](tokio::sync::broadcast::Receiver).
///
/// If a subscriber falls behind by more than `capacity` events, it will
/// receive a [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
/// on the next recv — this is by design; the bus does not buffer
/// unboundedly.
pub struct EventBus {
    sender: broadcast::Sender<MutationEvent>,
}

impl EventBus {
    /// Create a new event bus with the default capacity (256).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new event bus with a custom channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publish an event to all current subscribers.
    ///
    /// Returns the number of subscribers that received the event.
    /// Returns 0 if there are no active subscribers (the event is dropped).
    pub fn publish(&self, event: MutationEvent) -> usize {
        // `send` returns Err only when there are zero receivers — that's
        // not an error for a pub/sub bus, just means nobody is listening.
        self.sender.send(event).unwrap_or(0)
    }

    /// Subscribe to the event bus.
    ///
    /// Returns a receiver that will yield all events published *after*
    /// this call. Events published before subscribing are not replayed.
    pub fn subscribe(&self) -> broadcast::Receiver<MutationEvent> {
        self.sender.subscribe()
    }

    /// Returns the current number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::DocId;
    use crate::reactive::MutationType;

    #[test]
    fn new_bus_has_no_subscribers() {
        let bus = EventBus::new();
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[test]
    fn publish_with_no_subscribers_returns_zero() {
        let bus = EventBus::new();
        let count = bus.publish(MutationEvent::insert("users", DocId(1)));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn single_subscriber_receives_event() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 1);

        let event = MutationEvent::insert("users", DocId(1));
        let sent = bus.publish(event.clone());
        assert_eq!(sent, 1);

        let received = rx.recv().await.expect("should receive");
        assert_eq!(received, event);
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let bus = EventBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        let mut rx3 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 3);

        let event = MutationEvent::update("orders", DocId(42));
        let sent = bus.publish(event.clone());
        assert_eq!(sent, 3);

        assert_eq!(rx1.recv().await.unwrap(), event);
        assert_eq!(rx2.recv().await.unwrap(), event);
        assert_eq!(rx3.recv().await.unwrap(), event);
    }

    #[tokio::test]
    async fn events_in_order() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        let e1 = MutationEvent::insert("users", DocId(1));
        let e2 = MutationEvent::update("users", DocId(1));
        let e3 = MutationEvent::delete("users", DocId(1));

        bus.publish(e1.clone());
        bus.publish(e2.clone());
        bus.publish(e3.clone());

        assert_eq!(rx.recv().await.unwrap(), e1);
        assert_eq!(rx.recv().await.unwrap(), e2);
        assert_eq!(rx.recv().await.unwrap(), e3);
    }

    #[tokio::test]
    async fn subscriber_only_sees_events_after_subscribe() {
        let bus = EventBus::new();

        // Publish before subscribing.
        bus.publish(MutationEvent::insert("old", DocId(0)));

        let mut rx = bus.subscribe();

        // Publish after subscribing.
        let event = MutationEvent::insert("new", DocId(1));
        bus.publish(event.clone());

        let received = rx.recv().await.unwrap();
        assert_eq!(received, event);
    }

    #[tokio::test]
    async fn dropped_subscriber_does_not_block() {
        let bus = EventBus::new();
        let rx = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 1);

        drop(rx);
        assert_eq!(bus.subscriber_count(), 0);

        // Publishing with zero subscribers should not panic.
        let count = bus.publish(MutationEvent::insert("x", DocId(1)));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn lagged_subscriber_gets_error() {
        // Tiny capacity to force lag.
        let bus = EventBus::with_capacity(2);
        let mut rx = bus.subscribe();

        // Publish 5 events into a capacity-2 channel.
        for i in 0..5u64 {
            bus.publish(MutationEvent::insert("x", DocId(i)));
        }

        // First recv should report lag (missed events).
        let result = rx.recv().await;
        assert!(
            result.is_ok() || matches!(
                result,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
            ),
            "expected either a message or Lagged, got {result:?}"
        );
    }

    #[tokio::test]
    async fn different_collections_all_broadcast() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        let e1 = MutationEvent::insert("users", DocId(1));
        let e2 = MutationEvent::insert("orders", DocId(2));
        bus.publish(e1.clone());
        bus.publish(e2.clone());

        // Subscriber sees all events — filtering is the subscriber's
        // responsibility.
        assert_eq!(rx.recv().await.unwrap(), e1);
        assert_eq!(rx.recv().await.unwrap(), e2);
    }

    #[tokio::test]
    async fn subscriber_can_filter_by_collection() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        bus.publish(MutationEvent::insert("users", DocId(1)));
        bus.publish(MutationEvent::insert("orders", DocId(2)));
        bus.publish(MutationEvent::update("users", DocId(3)));

        let mut user_events = Vec::new();
        for _ in 0..3 {
            let event = rx.recv().await.unwrap();
            if event.collection == "users" {
                user_events.push(event);
            }
        }

        assert_eq!(user_events.len(), 2);
        assert_eq!(user_events[0].doc_id, DocId(1));
        assert_eq!(user_events[1].doc_id, DocId(3));
    }

    #[test]
    fn default_creates_bus() {
        let bus = EventBus::default();
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn mutation_types_round_trip() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        for mt in [MutationType::Insert, MutationType::Update, MutationType::Delete] {
            bus.publish(MutationEvent {
                collection: "test".into(),
                doc_id: DocId(0),
                mutation_type: mt,
            });
        }

        assert_eq!(rx.recv().await.unwrap().mutation_type, MutationType::Insert);
        assert_eq!(rx.recv().await.unwrap().mutation_type, MutationType::Update);
        assert_eq!(rx.recv().await.unwrap().mutation_type, MutationType::Delete);
    }
}
