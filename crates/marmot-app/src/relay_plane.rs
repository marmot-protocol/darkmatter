use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use cgka_traits::transport::Timestamp;
use cgka_traits::{
    MemberId, TransportAccountActivation, TransportAdapter, TransportAdapterError,
    TransportDelivery, TransportGroupSync, TransportPublishReport, TransportPublishRequest,
};
use nostr_sdk::prelude::Client as NostrSdkClient;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use transport_nostr_adapter::{
    NostrPublishOutcome, NostrRelayClient, NostrSdkRelayClient, NostrTransportAdapter,
};
use transport_nostr_peeler::NostrTransportEvent;

const ACCOUNT_DELIVERY_BUFFER: usize = 1024;

#[derive(Clone)]
pub struct MarmotRelayPlane {
    inner: Arc<MarmotRelayPlaneInner>,
}

struct MarmotRelayPlaneInner {
    subscription_rebuild_lookback: Option<Duration>,
    transport: Arc<RelayPlaneTransport>,
}

struct RelayPlaneTransport {
    adapter: NostrTransportAdapter,
    sdk_relay_client: Option<NostrSdkRelayClient>,
    account_deliveries: RwLock<HashMap<MemberId, mpsc::Sender<TransportDelivery>>>,
    router: Mutex<Option<JoinHandle<()>>>,
    notification_forwarder: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct MarmotRelayPlaneAccountAdapter {
    account_id: MemberId,
    relay_plane: MarmotRelayPlane,
    publish_client: Arc<dyn NostrRelayClient>,
    delivery_rx: Arc<Mutex<mpsc::Receiver<TransportDelivery>>>,
}

impl MarmotRelayPlane {
    pub fn runtime_default(subscription_rebuild_lookback: Duration) -> Self {
        Self::from_sdk(Some(subscription_rebuild_lookback))
    }

    pub fn full_history() -> Self {
        Self::from_sdk(None)
    }

    pub fn with_subscription_rebuild_lookback(lookback: Duration) -> Self {
        Self::from_sdk(Some(lookback))
    }

    pub fn new(
        subscription_rebuild_lookback: Option<Duration>,
        relay_client: Arc<dyn NostrRelayClient>,
    ) -> Self {
        let adapter = NostrTransportAdapter::new(relay_client);
        Self::from_adapter(subscription_rebuild_lookback, adapter, None, None)
    }

    fn from_sdk(subscription_rebuild_lookback: Option<Duration>) -> Self {
        let relay_client = NostrSdkRelayClient::new(NostrSdkClient::builder().build());
        let adapter = NostrTransportAdapter::new(Arc::new(relay_client.clone()));
        Self::from_adapter(
            subscription_rebuild_lookback,
            adapter,
            Some(relay_client),
            None,
        )
    }

    fn from_adapter(
        subscription_rebuild_lookback: Option<Duration>,
        adapter: NostrTransportAdapter,
        sdk_relay_client: Option<NostrSdkRelayClient>,
        notification_forwarder: Option<JoinHandle<()>>,
    ) -> Self {
        let transport = Arc::new(RelayPlaneTransport {
            adapter,
            sdk_relay_client,
            account_deliveries: RwLock::new(HashMap::new()),
            router: Mutex::new(None),
            notification_forwarder: Mutex::new(notification_forwarder),
        });
        let this = Self {
            inner: Arc::new(MarmotRelayPlaneInner {
                subscription_rebuild_lookback,
                transport,
            }),
        };
        this.spawn_router();
        this
    }

    pub fn account_adapter(
        &self,
        account_id: MemberId,
        publish_client: Arc<dyn NostrRelayClient>,
    ) -> MarmotRelayPlaneAccountAdapter {
        self.spawn_router();
        let (delivery_tx, delivery_rx) = mpsc::channel(ACCOUNT_DELIVERY_BUFFER);
        self.inner
            .transport
            .account_deliveries
            .write()
            .expect("relay-plane account deliveries lock poisoned")
            .insert(account_id.clone(), delivery_tx);
        MarmotRelayPlaneAccountAdapter {
            account_id,
            relay_plane: self.clone(),
            publish_client,
            delivery_rx: Arc::new(Mutex::new(delivery_rx)),
        }
    }

    pub fn subscription_rebuild_since(
        &self,
        last_transport_timestamp: Option<u64>,
    ) -> Option<Timestamp> {
        let lookback = self.inner.subscription_rebuild_lookback?;
        let last_transport_timestamp = last_transport_timestamp?;
        Some(Timestamp(
            last_transport_timestamp.saturating_sub(lookback.as_secs()),
        ))
    }

    pub async fn shutdown(&self) {
        if let Some(handle) = self.inner.transport.router.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self
            .inner
            .transport
            .notification_forwarder
            .lock()
            .await
            .take()
        {
            handle.abort();
        }
    }

    fn spawn_router(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let Ok(mut router) = self.inner.transport.router.try_lock() else {
            return;
        };
        if router.is_some() {
            return;
        }
        if let Ok(mut notification_forwarder) =
            self.inner.transport.notification_forwarder.try_lock()
            && notification_forwarder.is_none()
            && let Some(sdk_relay_client) = &self.inner.transport.sdk_relay_client
        {
            *notification_forwarder = Some(
                sdk_relay_client
                    .clone()
                    .spawn_notification_forwarder(self.inner.transport.adapter.clone()),
            );
        }
        let transport = self.inner.transport.clone();
        let adapter = transport.adapter.clone();
        let handle = handle.spawn(async move {
            while let Ok(Some(delivery)) = adapter.receive().await {
                let sender = transport
                    .account_deliveries
                    .read()
                    .expect("relay-plane account deliveries lock poisoned")
                    .get(&delivery.account_id)
                    .cloned();
                if let Some(sender) = sender {
                    let _ = sender.send(delivery).await;
                }
            }
        });
        *router = Some(handle);
    }

    #[cfg(test)]
    pub(crate) async fn handle_relay_event_for_test(
        &self,
        relay_event: transport_nostr_adapter::NostrRelayEvent,
    ) -> Result<usize, TransportAdapterError> {
        self.inner
            .transport
            .adapter
            .handle_relay_event(relay_event)
            .await
    }
}

#[async_trait]
impl TransportAdapter for MarmotRelayPlaneAccountAdapter {
    async fn activate_account(
        &self,
        activation: TransportAccountActivation,
    ) -> Result<(), TransportAdapterError> {
        if activation.account_id != self.account_id {
            return Err(TransportAdapterError::AccountNotActive(
                activation.account_id,
            ));
        }
        self.relay_plane
            .inner
            .transport
            .adapter
            .activate_account(activation)
            .await
    }

    async fn sync_account_groups(
        &self,
        sync: TransportGroupSync,
    ) -> Result<(), TransportAdapterError> {
        if sync.account_id != self.account_id {
            return Err(TransportAdapterError::AccountNotActive(sync.account_id));
        }
        self.relay_plane
            .inner
            .transport
            .adapter
            .sync_account_groups(sync)
            .await
    }

    async fn deactivate_account(&self, account_id: &MemberId) -> Result<(), TransportAdapterError> {
        self.relay_plane
            .inner
            .transport
            .account_deliveries
            .write()
            .expect("relay-plane account deliveries lock poisoned")
            .remove(account_id);
        self.relay_plane
            .inner
            .transport
            .adapter
            .deactivate_account(account_id)
            .await
    }

    async fn publish(
        &self,
        request: TransportPublishRequest,
    ) -> Result<TransportPublishReport, TransportAdapterError> {
        if request.account_id != self.account_id {
            return Err(TransportAdapterError::AccountNotActive(request.account_id));
        }
        request.validate_envelope_matches_target()?;
        let event = NostrTransportEvent::from_transport_message(&request.message)
            .map_err(|e| TransportAdapterError::Publish(format!("Nostr payload: {e}")))?;
        let outcome = self
            .publish_client
            .publish_event(request.target.endpoints(), &event, request.required_acks)
            .await?;
        Ok(publish_report_from_outcome(outcome, request))
    }

    async fn receive(&self) -> Result<Option<TransportDelivery>, TransportAdapterError> {
        Ok(self.delivery_rx.lock().await.recv().await)
    }
}

fn publish_report_from_outcome(
    outcome: NostrPublishOutcome,
    request: TransportPublishRequest,
) -> TransportPublishReport {
    TransportPublishReport {
        message_id: outcome.message_id.unwrap_or(request.message.id),
        accepted: outcome.accepted,
        failed: outcome.failed,
        required_acks: request.required_acks,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use cgka_traits::transport::{TransportEnvelope, TransportMessage, TransportSource};
    use cgka_traits::{
        GroupId, MessageId, TransportDeliveryPlane, TransportEndpoint, TransportEndpointFailure,
        TransportEndpointReceipt, TransportGroupSubscription,
    };
    use transport_nostr_adapter::{NostrRelayEvent, NostrSubscription};
    use transport_nostr_peeler::{KIND_MARMOT_GROUP_MESSAGE, NOSTR_SOURCE};

    use super::*;

    #[derive(Default)]
    struct RecordingRelayClient {
        subscriptions: StdMutex<Vec<NostrSubscription>>,
        unsubscribed: StdMutex<Vec<NostrSubscription>>,
        unsubscribed_accounts: StdMutex<Vec<MemberId>>,
    }

    #[async_trait]
    impl NostrRelayClient for RecordingRelayClient {
        async fn subscribe(
            &self,
            subscription: NostrSubscription,
        ) -> Result<(), TransportAdapterError> {
            self.subscriptions.lock().unwrap().push(subscription);
            Ok(())
        }

        async fn unsubscribe(
            &self,
            subscription: NostrSubscription,
        ) -> Result<(), TransportAdapterError> {
            self.unsubscribed.lock().unwrap().push(subscription);
            Ok(())
        }

        async fn unsubscribe_account(
            &self,
            account_id: &MemberId,
        ) -> Result<(), TransportAdapterError> {
            self.unsubscribed_accounts
                .lock()
                .unwrap()
                .push(account_id.clone());
            Ok(())
        }

        async fn publish_event(
            &self,
            _endpoints: &[TransportEndpoint],
            _event: &NostrTransportEvent,
            _required_acks: usize,
        ) -> Result<NostrPublishOutcome, TransportAdapterError> {
            Ok(NostrPublishOutcome {
                message_id: None,
                accepted: Vec::<TransportEndpointReceipt>::new(),
                failed: Vec::<TransportEndpointFailure>::new(),
            })
        }
    }

    #[tokio::test]
    async fn group_subscriptions_remain_account_scoped_for_shared_group_routes() {
        let relay = Arc::new(RecordingRelayClient::default());
        let relay_plane = MarmotRelayPlane::new(Some(Duration::from_secs(30)), relay.clone());
        let alice = MemberId::new(vec![0xA1; 32]);
        let bob = MemberId::new(vec![0xB2; 32]);
        let group_id = GroupId::new(vec![0xC3; 32]);
        let transport_group_id = vec![0xD4; 32];
        let endpoint = TransportEndpoint("wss://relay.example".into());
        let alice_adapter = relay_plane.account_adapter(alice.clone(), relay.clone());
        let bob_adapter = relay_plane.account_adapter(bob.clone(), relay.clone());

        alice_adapter
            .activate_account(TransportAccountActivation {
                account_id: alice.clone(),
                inbox_endpoints: vec![endpoint.clone()],
                group_subscriptions: vec![TransportGroupSubscription {
                    group_id: group_id.clone(),
                    transport_group_id: transport_group_id.clone(),
                    endpoints: vec![endpoint.clone()],
                }],
                since: Some(Timestamp(10)),
            })
            .await
            .unwrap();
        bob_adapter
            .activate_account(TransportAccountActivation {
                account_id: bob.clone(),
                inbox_endpoints: vec![endpoint.clone()],
                group_subscriptions: vec![TransportGroupSubscription {
                    group_id: group_id.clone(),
                    transport_group_id: transport_group_id.clone(),
                    endpoints: vec![endpoint.clone()],
                }],
                since: Some(Timestamp(10)),
            })
            .await
            .unwrap();

        let subscriptions = relay.subscriptions.lock().unwrap().clone();
        let group_subscriptions = subscriptions
            .iter()
            .filter(|subscription| matches!(subscription, NostrSubscription::Group { .. }))
            .collect::<Vec<_>>();
        assert_eq!(group_subscriptions.len(), 2);
        assert!(group_subscriptions.iter().any(|subscription| matches!(
            subscription,
            NostrSubscription::Group { account_id, .. } if account_id == &alice
        )));
        assert!(group_subscriptions.iter().any(|subscription| matches!(
            subscription,
            NostrSubscription::Group { account_id, .. } if account_id == &bob
        )));
    }

    #[tokio::test]
    async fn shared_group_event_is_delivered_to_each_matching_account_receiver() {
        let relay = Arc::new(RecordingRelayClient::default());
        let relay_plane = MarmotRelayPlane::new(Some(Duration::from_secs(30)), relay.clone());
        let alice = MemberId::new(vec![0xA1; 32]);
        let bob = MemberId::new(vec![0xB2; 32]);
        let group_id = GroupId::new(vec![0xC3; 32]);
        let transport_group_id = vec![0xD4; 32];
        let endpoint = TransportEndpoint("wss://relay.example".into());
        let alice_adapter = relay_plane.account_adapter(alice.clone(), relay.clone());
        let bob_adapter = relay_plane.account_adapter(bob.clone(), relay.clone());
        let subscription = TransportGroupSubscription {
            group_id: group_id.clone(),
            transport_group_id: transport_group_id.clone(),
            endpoints: vec![endpoint.clone()],
        };

        alice_adapter
            .activate_account(TransportAccountActivation {
                account_id: alice.clone(),
                inbox_endpoints: vec![endpoint.clone()],
                group_subscriptions: vec![subscription.clone()],
                since: None,
            })
            .await
            .unwrap();
        bob_adapter
            .activate_account(TransportAccountActivation {
                account_id: bob.clone(),
                inbox_endpoints: vec![endpoint.clone()],
                group_subscriptions: vec![subscription],
                since: None,
            })
            .await
            .unwrap();

        let delivered = relay_plane
            .handle_relay_event_for_test(NostrRelayEvent {
                endpoint,
                subscription_id: Some("group-sub".into()),
                event: group_event("11", &transport_group_id),
            })
            .await
            .unwrap();
        assert_eq!(delivered, 2);

        let alice_delivery = alice_adapter.receive().await.unwrap().unwrap();
        let bob_delivery = bob_adapter.receive().await.unwrap().unwrap();
        assert_eq!(alice_delivery.account_id, alice);
        assert_eq!(bob_delivery.account_id, bob);
        assert_eq!(alice_delivery.group_id_hint, Some(group_id.clone()));
        assert_eq!(bob_delivery.group_id_hint, Some(group_id));
        assert_eq!(alice_delivery.source.plane, TransportDeliveryPlane::Group);
        assert_eq!(bob_delivery.source.plane, TransportDeliveryPlane::Group);
    }

    fn group_event(id_prefix: &str, transport_group_id: &[u8]) -> NostrTransportEvent {
        NostrTransportEvent {
            id: id_prefix.repeat(32),
            pubkey: "22".repeat(32),
            created_at: 1_700_000_000,
            kind: KIND_MARMOT_GROUP_MESSAGE,
            tags: vec![vec!["h".into(), hex::encode(transport_group_id)]],
            content: "encrypted".into(),
            sig: None,
        }
    }

    #[test]
    fn publish_report_preserves_fallback_message_id() {
        let request = TransportPublishRequest {
            account_id: MemberId::new(vec![0xA1; 32]),
            message: TransportMessage {
                id: MessageId::new(vec![0x55; 32]),
                payload: Vec::new(),
                timestamp: Timestamp(1),
                causal_deps: Vec::new(),
                source: TransportSource(NOSTR_SOURCE.into()),
                envelope: TransportEnvelope::GroupMessage {
                    transport_group_id: vec![0x11],
                },
            },
            target: cgka_traits::TransportPublishTarget::Group {
                group_id: GroupId::new(vec![0x22; 32]),
                transport_group_id: vec![0x11],
                endpoints: Vec::new(),
            },
            required_acks: 2,
        };
        let report = publish_report_from_outcome(
            NostrPublishOutcome {
                message_id: None,
                accepted: Vec::new(),
                failed: Vec::new(),
            },
            request,
        );
        assert_eq!(report.message_id.as_slice(), vec![0x55; 32].as_slice());
        assert_eq!(report.required_acks, 2);
    }
}
