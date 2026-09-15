from pathlib import Path

p = Path('crates/agent-runtime/src/agent/driver/recovery.rs')
s = p.read_text()
a = s.index('            // The fallback is local recovery')
b = s.index('            self.publish_terminal(', a)
s = s[:a] + '''            // The fallback is local recovery, not another turn. In particular
            // do not invoke a summary/model/tool hook merely by opening it.
            self.close_and_discard_steers(discard_reason_for_finish(&finish));
            // A Completing record already owns its finish and provider-error
            // classification. Rewriting that payload is not an idempotent
            // transition; advance it directly to publication instead.
            if !matches!(checkpoint.state, TurnState::Completing { .. } | TurnState::PublishingTerminal { .. }) {
                if let Err(error) = self.transition(TurnState::Completing {
                    finish: finish.clone(), visible_output: checkpoint.visible_output,
                    provider_error_kind: None,
                }).await {
                    self.emit_non_durable_failure(error, checkpoint.visible_output);
                    return;
                }
            }
            if !matches!(checkpoint.state, TurnState::PublishingTerminal { .. }) {
                if let Err(error) = self.transition(TurnState::PublishingTerminal {
                    finish: finish.clone(), visible_output: checkpoint.visible_output,
                }).await {
                    self.emit_non_durable_failure(error, checkpoint.visible_output);
                    return;
                }
            }
''' + s[b:]
p.write_text(s)

p = Path('crates/agent-runtime/src/harness/live_abilities/session.rs')
s = p.read_text()
marker = 'impl SessionAbilities {\n'
assert s.count(marker) == 1
s = s.replace(marker, marker + '''    /// An interrupted turn cannot promote its unfinished activation work.
    /// Already active capabilities remain subject to the normal scoped rebase.
    pub(crate) fn discard_uncommitted_activation(&self) {
        let mut state = self.state.lock().expect("activation state poisoned");
        state.pending.clear();
        state.staged.clear();
    }

''')
p.write_text(s)
p = Path('crates/agent-runtime/src/runtime/engine.rs')
s = p.read_text()
marker = '        let persist_gate = execution.persist_gate();'
assert s.count(marker) == 1
s = s.replace(marker, '''        if interrupted_on_resume.is_some() {
            if let Some(abilities) = &execution.abilities {
                abilities.discard_uncommitted_activation();
            }
        }
''' + marker)
p.write_text(s)

p = Path('crates/agent-runtime/tests/interrupted_turn_admission.rs')
s = p.read_text()
assert 'upgrade_recovery_preserves_a_previously_decided_finish' not in s
s += '''
#[tokio::test]
async fn upgrade_recovery_preserves_a_previously_decided_finish() {
    for publishing in [false, true] {
        let (id, sessions, checkpoints, original) = saved_upgrade_turn().await;
        let mut checkpoint = original.transition(
            TurnState::Completing {
                finish: TurnFinish::Failed,
                visible_output: false,
                provider_error_kind: Some(agent_runtime::core::provider::ProviderErrorKind::Network),
            },
            original.snapshot.clone(), original.watermark.event_sequence + 1, Timestamp(1),
        ).unwrap();
        if publishing {
            checkpoint = checkpoint.transition(
                TurnState::PublishingTerminal { finish: TurnFinish::Failed, visible_output: false },
                checkpoint.snapshot.clone(), checkpoint.watermark.event_sequence + 1, Timestamp(2),
            ).unwrap();
        }
        checkpoints.seed(checkpoint.clone());
        let provider = Arc::new(FakeProvider::new("fake", Capabilities::basic_streaming(), vec![]));
        let runtime = upgrade_runtime(provider.clone(), sessions, checkpoints.clone(), true);
        let resumed = runtime.start_session(StartSession::new().with_id(id)
            .with_checkpoint_recovery(CheckpointRecoveryPolicy::ResumeOrInterrupt)).await.unwrap();
        assert_eq!(resumed.interrupted_on_resume(), Some(&checkpoint.turn));
        assert_eq!(resumed.history(), checkpoint.snapshot.history);
        assert!(provider.requests().is_empty());
        assert!(matches!(checkpoints.latest().unwrap().state, TurnState::Terminal { finish: TurnFinish::Failed, .. }));
        resumed.shutdown().await.unwrap();
    }
}
'''
p.write_text(s)
