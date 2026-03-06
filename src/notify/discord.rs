use super::{BackupFailureEvent, Notifier};
use async_trait::async_trait;
use reqwest::Client;
use serde::Serialize;
use tracing::{error, warn};

#[derive(Debug, Clone)]
pub struct DiscordNotifier {
    webhook_url: String,
    role_mention: Option<String>,
    client: Client,
}

impl DiscordNotifier {
    pub fn new(webhook_url: String, role_mention: Option<String>) -> Self {
        Self {
            webhook_url,
            role_mention,
            client: Client::new(),
        }
    }

    fn build_payload(&self, event: &BackupFailureEvent) -> WebhookPayload {
        let mention = self.role_mention.clone().unwrap_or_default();
        let content = if mention.is_empty() {
            format!("Mongo backup failed: run `{}`", event.run_id)
        } else {
            format!("{mention} Mongo backup failed: run `{}`", event.run_id)
        };

        WebhookPayload {
            content,
            embeds: vec![WebhookEmbed {
                title: "Mongo Backup Failure".to_owned(),
                description: "A scheduled or manual backup run failed.".to_owned(),
                color: 15_385_343,
                fields: vec![
                    WebhookField::inline("Run ID", event.run_id.clone()),
                    WebhookField::inline("Stage", event.stage.clone()),
                    WebhookField::inline("Timestamp (UTC)", event.occurred_at_utc.to_rfc3339()),
                    WebhookField::block("Error", truncate(&event.error, 900)),
                ],
            }],
        }
    }
}

#[async_trait]
impl Notifier for DiscordNotifier {
    async fn notify_backup_failure(&self, event: &BackupFailureEvent) {
        let payload = self.build_payload(event);
        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                warn!(status = %resp.status(), "discord webhook returned non-success status")
            }
            Err(err) => error!(error = %err, "failed to send discord webhook notification"),
        }
    }
}

fn truncate(raw: &str, max_chars: usize) -> String {
    if raw.chars().count() <= max_chars {
        return raw.to_owned();
    }
    let trimmed = raw.chars().take(max_chars).collect::<String>();
    format!("{trimmed}...")
}

#[derive(Debug, Serialize)]
struct WebhookPayload {
    content: String,
    embeds: Vec<WebhookEmbed>,
}

#[derive(Debug, Serialize)]
struct WebhookEmbed {
    title: String,
    description: String,
    color: u32,
    fields: Vec<WebhookField>,
}

#[derive(Debug, Serialize)]
struct WebhookField {
    name: String,
    value: String,
    inline: bool,
}

impl WebhookField {
    fn inline(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            inline: true,
        }
    }

    fn block(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            inline: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn payload_includes_optional_role_mention() {
        let notifier = DiscordNotifier::new(
            "https://discord.com/api/webhooks/test".to_owned(),
            Some("<@&12345>".to_owned()),
        );

        let event = BackupFailureEvent {
            run_id: "20260304T000000Z-abcd1234".to_owned(),
            stage: "upload".to_owned(),
            error: "boom".to_owned(),
            occurred_at_utc: Utc::now(),
        };

        let payload = notifier.build_payload(&event);
        assert!(payload.content.contains("<@&12345>"));
        assert_eq!(payload.embeds[0].fields[0].name, "Run ID");
    }
}
