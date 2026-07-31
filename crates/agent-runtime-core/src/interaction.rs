//! Bounded, authority-free agent-to-host interaction contracts.
//!
//! Interaction carries task information only. It is deliberately disjoint
//! from approval, permissions, and grants: an answer can become a canonical
//! tool result, but can never authorize a side effect.

use std::collections::BTreeSet;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_registry::Fingerprint;

use crate::clock::Deadline;
use crate::error::RuntimeError;
use crate::ids::{ChoiceId, InteractionRequestId, QuestionId, SessionId, ToolCallId, TurnId};

/// Interaction request/response wire schema.
pub const INTERACTION_SCHEMA_VERSION: u32 = 1;
/// Maximum questions in one questionnaire.
pub const MAX_QUESTIONS: usize = 3;
/// Maximum choices in one question.
pub const MAX_CHOICES_PER_QUESTION: usize = 8;
/// Maximum Unicode scalar values in a prompt.
pub const MAX_QUESTION_CHARS: usize = 1_024;
/// Maximum Unicode scalar values in a short question header.
pub const MAX_QUESTION_HEADER_CHARS: usize = 64;
/// Maximum Unicode scalar values in a choice label.
pub const MAX_CHOICE_LABEL_CHARS: usize = 200;
/// Maximum Unicode scalar values in a choice description.
pub const MAX_CHOICE_DESCRIPTION_CHARS: usize = 512;
/// Maximum Unicode scalar values in one free-form answer.
pub const MAX_FREE_FORM_CHARS: usize = 8_192;
const MAX_ID_CHARS: usize = 128;
const MAX_UNAVAILABLE_REASON_CHARS: usize = 512;

/// Origin attribution for one interaction.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionOrigin {
    session: SessionId,
    turn: TurnId,
    call: ToolCallId,
}

impl InteractionOrigin {
    /// Creates fully attributed origin identity.
    pub fn new(session: SessionId, turn: TurnId, call: ToolCallId) -> Self {
        Self {
            session,
            turn,
            call,
        }
    }

    /// Owning session.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// Owning turn.
    pub fn turn(&self) -> &TurnId {
        &self.turn
    }

    /// Questionnaire tool call producing the interaction.
    pub fn call(&self) -> &ToolCallId {
        &self.call
    }
}

impl fmt::Debug for InteractionOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InteractionOrigin")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("call", &self.call)
            .finish()
    }
}

/// Content handling requested for an interaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionSensitivity {
    /// Ordinary task content.
    #[default]
    Public,
    /// Prompt/answer content must remain out of default events and logs.
    Sensitive,
}

/// One mutually exclusive choice.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Choice {
    id: ChoiceId,
    label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl Choice {
    /// Creates a choice.
    pub fn new(id: ChoiceId, label: impl Into<String>) -> Self {
        Self {
            id,
            label: label.into(),
            description: None,
        }
    }

    /// Adds explanatory text.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Stable choice identity.
    pub fn id(&self) -> &ChoiceId {
        &self.id
    }

    /// Human-facing label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Optional human-facing explanation.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

impl fmt::Debug for Choice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Choice")
            .field("label_chars", &self.label.chars().count())
            .field(
                "description_chars",
                &self.description.as_ref().map(|value| value.chars().count()),
            )
            .finish()
    }
}

/// One required questionnaire question.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    id: QuestionId,
    header: String,
    prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    choices: Vec<Choice>,
    #[serde(default)]
    allow_free_form: bool,
}

impl Question {
    /// Creates a question.
    pub fn new(id: QuestionId, header: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            id,
            header: header.into(),
            prompt: prompt.into(),
            choices: Vec::new(),
            allow_free_form: false,
        }
    }

    /// Sets mutually exclusive choices.
    pub fn with_choices(mut self, choices: Vec<Choice>) -> Self {
        self.choices = choices;
        self
    }

    /// Enables or disables a free-form answer in place of a choice.
    pub fn allow_free_form(mut self, allow: bool) -> Self {
        self.allow_free_form = allow;
        self
    }

    /// Stable question identity.
    pub fn id(&self) -> &QuestionId {
        &self.id
    }

    /// Human-facing question.
    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    /// Short host-facing header.
    pub fn header(&self) -> &str {
        &self.header
    }

    /// Mutually exclusive choices.
    pub fn choices(&self) -> &[Choice] {
        &self.choices
    }

    /// Whether a free-form answer is accepted.
    pub fn allows_free_form(&self) -> bool {
        self.allow_free_form
    }
}

impl fmt::Debug for Question {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Question")
            .field("header_chars", &self.header.chars().count())
            .field("prompt_chars", &self.prompt.chars().count())
            .field("choice_count", &self.choices.len())
            .field("allow_free_form", &self.allow_free_form)
            .finish()
    }
}

/// A validated one-to-three-question questionnaire.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Questionnaire {
    questions: Vec<Question>,
}

impl Questionnaire {
    /// Validates and creates a questionnaire.
    pub fn new(questions: Vec<Question>) -> Result<Self, RuntimeError> {
        let questionnaire = Self { questions };
        questionnaire.validate()?;
        Ok(questionnaire)
    }

    /// Questions in stable presentation/answer order.
    pub fn questions(&self) -> &[Question] {
        &self.questions
    }

    /// Revalidates deserialized content.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if !(1..=MAX_QUESTIONS).contains(&self.questions.len()) {
            return Err(RuntimeError::config(format!(
                "questionnaire must contain one to {MAX_QUESTIONS} questions"
            )));
        }
        let mut question_ids = BTreeSet::new();
        for question in &self.questions {
            validate_id("question", question.id.as_str())?;
            if !question_ids.insert(question.id.clone()) {
                return Err(RuntimeError::config(format!(
                    "duplicate question id `{}`",
                    question.id
                )));
            }
            validate_text(
                "question header",
                &question.header,
                MAX_QUESTION_HEADER_CHARS,
            )?;
            validate_text("question prompt", &question.prompt, MAX_QUESTION_CHARS)?;
            if question.choices.is_empty() && !question.allow_free_form {
                return Err(RuntimeError::config(format!(
                    "question `{}` has neither choices nor free-form input",
                    question.id
                )));
            }
            if question.choices.len() > MAX_CHOICES_PER_QUESTION {
                return Err(RuntimeError::config(format!(
                    "question `{}` exceeds {MAX_CHOICES_PER_QUESTION} choices",
                    question.id
                )));
            }
            let mut choice_ids = BTreeSet::new();
            for choice in &question.choices {
                validate_id("choice", choice.id.as_str())?;
                if !choice_ids.insert(choice.id.clone()) {
                    return Err(RuntimeError::config(format!(
                        "duplicate choice id `{}` in question `{}`",
                        choice.id, question.id
                    )));
                }
                validate_text("choice label", &choice.label, MAX_CHOICE_LABEL_CHARS)?;
                if let Some(description) = &choice.description {
                    validate_text(
                        "choice description",
                        description,
                        MAX_CHOICE_DESCRIPTION_CHARS,
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Questionnaire {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Questionnaire")
            .field("question_count", &self.questions.len())
            .finish()
    }
}

/// Exact, fingerprinted request presented to an interaction host.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct InteractionRequest {
    /// Wire schema.
    schema_version: u32,
    id: InteractionRequestId,
    origin: InteractionOrigin,
    questionnaire: Questionnaire,
    deadline: Deadline,
    sensitivity: InteractionSensitivity,
    fingerprint: Fingerprint,
}

impl InteractionRequest {
    /// Creates and fingerprints a questionnaire request.
    pub fn questionnaire(
        id: InteractionRequestId,
        origin: InteractionOrigin,
        questionnaire: Questionnaire,
        deadline: Deadline,
        sensitivity: InteractionSensitivity,
    ) -> Result<Self, RuntimeError> {
        let fingerprint = request_fingerprint(
            INTERACTION_SCHEMA_VERSION,
            &id,
            &origin,
            &questionnaire,
            deadline,
            sensitivity,
        );
        let request = Self {
            schema_version: INTERACTION_SCHEMA_VERSION,
            id,
            origin,
            questionnaire,
            deadline,
            sensitivity,
            fingerprint,
        };
        request.validate()?;
        Ok(request)
    }

    /// Request identity.
    pub fn id(&self) -> &InteractionRequestId {
        &self.id
    }

    /// Session/turn/call attribution.
    pub fn origin(&self) -> &InteractionOrigin {
        &self.origin
    }

    /// Questionnaire payload.
    pub fn questionnaire_payload(&self) -> &Questionnaire {
        &self.questionnaire
    }

    /// Absolute interaction deadline.
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Content sensitivity.
    pub fn sensitivity(&self) -> InteractionSensitivity {
        self.sensitivity
    }

    /// Stable exact-request fingerprint.
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }

    /// Revalidates schema, bounds, attribution, and fingerprint.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != INTERACTION_SCHEMA_VERSION {
            return Err(RuntimeError::config(format!(
                "unsupported interaction schema {}; expected {}",
                self.schema_version, INTERACTION_SCHEMA_VERSION
            )));
        }
        validate_id("interaction request", self.id.as_str())?;
        validate_id("interaction session", self.origin.session.as_str())?;
        validate_id("interaction turn", self.origin.turn.as_str())?;
        validate_id("interaction call", self.origin.call.as_str())?;
        self.questionnaire.validate()?;
        if self.fingerprint
            != request_fingerprint(
                self.schema_version,
                &self.id,
                &self.origin,
                &self.questionnaire,
                self.deadline,
                self.sensitivity,
            )
        {
            return Err(RuntimeError::conflict(
                "interaction request fingerprint mismatch",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for InteractionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InteractionRequest")
            .field("schema_version", &self.schema_version)
            .field("id", &self.id)
            .field("origin", &self.origin)
            .field("question_count", &self.questionnaire.questions.len())
            .field("deadline", &self.deadline)
            .field("sensitivity", &self.sensitivity)
            .finish()
    }
}

/// One answer to one question.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionAnswer {
    question_id: QuestionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    choice_id: Option<ChoiceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    free_form: Option<String>,
}

impl QuestionAnswer {
    /// Selects one choice.
    pub fn choice(question_id: QuestionId, choice_id: ChoiceId) -> Self {
        Self {
            question_id,
            choice_id: Some(choice_id),
            free_form: None,
        }
    }

    /// Supplies one free-form answer.
    pub fn free_form(question_id: QuestionId, answer: impl Into<String>) -> Self {
        Self {
            question_id,
            choice_id: None,
            free_form: Some(answer.into()),
        }
    }

    /// Answered question.
    pub fn question_id(&self) -> &QuestionId {
        &self.question_id
    }

    /// Selected choice, if any.
    pub fn choice_id(&self) -> Option<&ChoiceId> {
        self.choice_id.as_ref()
    }

    /// Free-form value, if any.
    pub fn free_form_value(&self) -> Option<&str> {
        self.free_form.as_deref()
    }
}

impl fmt::Debug for QuestionAnswer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuestionAnswer")
            .field(
                "answer_kind",
                &if self.choice_id.is_some() {
                    "choice"
                } else {
                    "free_form"
                },
            )
            .finish()
    }
}

/// Versioned authority-free interaction response.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionResponse {
    schema_version: u32,
    #[serde(flatten)]
    outcome: InteractionResponseKind,
}

/// Authority-free response outcome payload.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum InteractionResponseKind {
    /// Every required question was answered.
    Answered {
        /// Request being answered.
        request_id: InteractionRequestId,
        /// Answers in request question order.
        answers: Vec<QuestionAnswer>,
    },
    /// The user explicitly declined to answer.
    Declined {
        /// Request being declined.
        request_id: InteractionRequestId,
    },
    /// Runtime deadline elapsed.
    TimedOut {
        /// Request that timed out.
        request_id: InteractionRequestId,
    },
    /// Turn/session cancellation won.
    Cancelled {
        /// Request that was cancelled.
        request_id: InteractionRequestId,
    },
    /// No compatible interaction host was available.
    Unavailable {
        /// Request that could not be presented.
        request_id: InteractionRequestId,
        /// Bounded redaction-safe reason.
        reason: String,
    },
}

impl fmt::Debug for InteractionResponseKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Answered {
                request_id,
                answers,
            } => formatter
                .debug_struct("Answered")
                .field("request_id", request_id)
                .field("answer_count", &answers.len())
                .finish(),
            Self::Declined { request_id } => formatter
                .debug_struct("Declined")
                .field("request_id", request_id)
                .finish(),
            Self::TimedOut { request_id } => formatter
                .debug_struct("TimedOut")
                .field("request_id", request_id)
                .finish(),
            Self::Cancelled { request_id } => formatter
                .debug_struct("Cancelled")
                .field("request_id", request_id)
                .finish(),
            Self::Unavailable { request_id, reason } => formatter
                .debug_struct("Unavailable")
                .field("request_id", request_id)
                .field("reason_chars", &reason.chars().count())
                .finish(),
        }
    }
}

impl InteractionResponse {
    /// Creates an answered outcome.
    pub fn answered(request_id: InteractionRequestId, answers: Vec<QuestionAnswer>) -> Self {
        Self {
            schema_version: INTERACTION_SCHEMA_VERSION,
            outcome: InteractionResponseKind::Answered {
                request_id,
                answers,
            },
        }
    }

    /// Creates a declined outcome.
    pub fn declined(request_id: InteractionRequestId) -> Self {
        Self {
            schema_version: INTERACTION_SCHEMA_VERSION,
            outcome: InteractionResponseKind::Declined { request_id },
        }
    }

    /// Creates a runtime timeout outcome.
    pub fn timed_out(request_id: InteractionRequestId) -> Self {
        Self {
            schema_version: INTERACTION_SCHEMA_VERSION,
            outcome: InteractionResponseKind::TimedOut { request_id },
        }
    }

    /// Creates a runtime cancellation outcome.
    pub fn cancelled(request_id: InteractionRequestId) -> Self {
        Self {
            schema_version: INTERACTION_SCHEMA_VERSION,
            outcome: InteractionResponseKind::Cancelled { request_id },
        }
    }

    /// Creates an unavailable-host outcome with a bounded reason.
    pub fn unavailable(request_id: InteractionRequestId, reason: impl Into<String>) -> Self {
        Self {
            schema_version: INTERACTION_SCHEMA_VERSION,
            outcome: InteractionResponseKind::Unavailable {
                request_id,
                reason: bound_string(reason.into(), MAX_UNAVAILABLE_REASON_CHARS),
            },
        }
    }

    /// Response wire schema.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Structured authority-free outcome.
    pub fn outcome(&self) -> &InteractionResponseKind {
        &self.outcome
    }

    /// Request identity carried by every outcome.
    pub fn request_id(&self) -> &InteractionRequestId {
        match &self.outcome {
            InteractionResponseKind::Answered { request_id, .. }
            | InteractionResponseKind::Declined { request_id }
            | InteractionResponseKind::TimedOut { request_id }
            | InteractionResponseKind::Cancelled { request_id }
            | InteractionResponseKind::Unavailable { request_id, .. } => request_id,
        }
    }

    /// Metadata-only outcome kind.
    pub fn outcome_kind(&self) -> InteractionOutcomeKind {
        match self.outcome {
            InteractionResponseKind::Answered { .. } => InteractionOutcomeKind::Answered,
            InteractionResponseKind::Declined { .. } => InteractionOutcomeKind::Declined,
            InteractionResponseKind::TimedOut { .. } => InteractionOutcomeKind::TimedOut,
            InteractionResponseKind::Cancelled { .. } => InteractionOutcomeKind::Cancelled,
            InteractionResponseKind::Unavailable { .. } => InteractionOutcomeKind::Unavailable,
        }
    }

    /// Answers, only for an answered outcome.
    pub fn answers(&self) -> Option<&[QuestionAnswer]> {
        match &self.outcome {
            InteractionResponseKind::Answered { answers, .. } => Some(answers),
            _ => None,
        }
    }

    /// Validates request identity and exact answer coverage.
    pub fn validate_for(&self, request: &InteractionRequest) -> Result<(), RuntimeError> {
        request.validate()?;
        if self.schema_version != INTERACTION_SCHEMA_VERSION {
            return Err(RuntimeError::config(format!(
                "unsupported interaction response schema {}; expected {}",
                self.schema_version, INTERACTION_SCHEMA_VERSION
            )));
        }
        if self.request_id() != request.id() {
            return Err(RuntimeError::conflict(
                "interaction response belongs to another request",
            ));
        }
        match &self.outcome {
            InteractionResponseKind::Answered { answers, .. } => {
                let questions = request.questionnaire.questions();
                if answers.len() != questions.len() {
                    return Err(RuntimeError::config(
                        "interaction response must answer every question exactly once",
                    ));
                }
                let mut answered = BTreeSet::new();
                for (answer, question) in answers.iter().zip(questions) {
                    if !answered.insert(answer.question_id.clone()) {
                        return Err(RuntimeError::config(format!(
                            "duplicate answer for question `{}`",
                            answer.question_id
                        )));
                    }
                    if answer.question_id != question.id {
                        return Err(RuntimeError::config(format!(
                            "answer for foreign or out-of-order question `{}`",
                            answer.question_id
                        )));
                    }
                    match (&answer.choice_id, &answer.free_form) {
                        (Some(choice), None) => {
                            if !question
                                .choices
                                .iter()
                                .any(|candidate| candidate.id == *choice)
                            {
                                return Err(RuntimeError::config(format!(
                                    "foreign choice `{choice}` for question `{}`",
                                    question.id
                                )));
                            }
                        }
                        (None, Some(value)) => {
                            if !question.allow_free_form {
                                return Err(RuntimeError::config(format!(
                                    "question `{}` does not allow free-form input",
                                    question.id
                                )));
                            }
                            validate_text("free-form answer", value, MAX_FREE_FORM_CHARS)?;
                        }
                        _ => {
                            return Err(RuntimeError::config(format!(
                                "question `{}` requires exactly one choice or free-form answer",
                                question.id
                            )));
                        }
                    }
                }
            }
            InteractionResponseKind::Unavailable { reason, .. } => {
                validate_text(
                    "interaction unavailable reason",
                    reason,
                    MAX_UNAVAILABLE_REASON_CHARS,
                )?;
            }
            InteractionResponseKind::Declined { .. }
            | InteractionResponseKind::TimedOut { .. }
            | InteractionResponseKind::Cancelled { .. } => {}
        }
        Ok(())
    }
}

impl fmt::Debug for InteractionResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.outcome {
            InteractionResponseKind::Answered {
                request_id,
                answers,
            } => formatter
                .debug_struct("InteractionResponse::Answered")
                .field("schema_version", &self.schema_version)
                .field("request_id", request_id)
                .field("answer_count", &answers.len())
                .finish(),
            InteractionResponseKind::Declined { request_id } => formatter
                .debug_struct("InteractionResponse::Declined")
                .field("schema_version", &self.schema_version)
                .field("request_id", request_id)
                .finish(),
            InteractionResponseKind::TimedOut { request_id } => formatter
                .debug_struct("InteractionResponse::TimedOut")
                .field("schema_version", &self.schema_version)
                .field("request_id", request_id)
                .finish(),
            InteractionResponseKind::Cancelled { request_id } => formatter
                .debug_struct("InteractionResponse::Cancelled")
                .field("schema_version", &self.schema_version)
                .field("request_id", request_id)
                .finish(),
            InteractionResponseKind::Unavailable { request_id, reason } => formatter
                .debug_struct("InteractionResponse::Unavailable")
                .field("schema_version", &self.schema_version)
                .field("request_id", request_id)
                .field("reason_chars", &reason.chars().count())
                .finish(),
        }
    }
}

/// Metadata-only resolution used to close a host prompt lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionOutcomeKind {
    /// Valid answers were accepted.
    Answered,
    /// User declined.
    Declined,
    /// Runtime deadline elapsed.
    TimedOut,
    /// Turn/session cancellation won.
    Cancelled,
    /// Host absent or returned an invalid/unavailable result.
    Unavailable,
}

/// Whether a broker can currently present requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionReadiness {
    /// Requests can be presented.
    Ready,
    /// Requests must return unavailable immediately.
    Unavailable,
}

/// Per-session handling for task-information requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InteractionDisposition {
    /// Present through the session's configured host broker.
    #[default]
    DirectHost,
    /// Complete the child tool exchange with metadata and return the exact
    /// request to its parent as a typed task outcome.
    ReturnToParent,
    /// Do not advertise interaction; a forced call resolves unavailable.
    Unavailable,
}

/// Host bridge for exact questionnaire requests.
///
/// The runtime enforces cancellation and deadline around `interact` even when
/// an implementation ignores them. It then calls [`InteractionBroker::close`]
/// synchronously and idempotently so queued/visible UI can be removed even
/// when the async future was dropped.
#[async_trait]
pub trait InteractionBroker: Send + Sync + fmt::Debug {
    /// Current activation/invocation readiness.
    fn readiness(&self) -> InteractionReadiness;

    /// Presents the exact borrowed request and waits for a task-information
    /// response.
    async fn interact(&self, request: &InteractionRequest) -> InteractionResponse;

    /// Closes one queued/visible prompt lifecycle. Implementations must make
    /// this idempotent and ignore late answers after closure.
    fn close(&self, request_id: &InteractionRequestId, outcome: InteractionOutcomeKind);
}

/// Fail-fast broker for non-interactive hosts.
#[derive(Debug, Default)]
pub struct UnavailableInteractionBroker;

#[async_trait]
impl InteractionBroker for UnavailableInteractionBroker {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Unavailable
    }

    async fn interact(&self, request: &InteractionRequest) -> InteractionResponse {
        InteractionResponse::unavailable(
            request.id().clone(),
            "no interaction broker is configured",
        )
    }

    fn close(&self, _request_id: &InteractionRequestId, _outcome: InteractionOutcomeKind) {}
}

fn request_fingerprint(
    schema_version: u32,
    id: &InteractionRequestId,
    origin: &InteractionOrigin,
    questionnaire: &Questionnaire,
    deadline: Deadline,
    sensitivity: InteractionSensitivity,
) -> Fingerprint {
    Fingerprint::of_fields([
        b"interaction_request".as_slice(),
        schema_version.to_string().as_bytes(),
        id.as_str().as_bytes(),
        &serde_json::to_vec(origin).unwrap_or_default(),
        &serde_json::to_vec(questionnaire).unwrap_or_default(),
        &serde_json::to_vec(&deadline).unwrap_or_default(),
        &serde_json::to_vec(&sensitivity).unwrap_or_default(),
    ])
}

fn validate_id(kind: &str, value: &str) -> Result<(), RuntimeError> {
    if value.is_empty() || value.chars().count() > MAX_ID_CHARS {
        return Err(RuntimeError::config(format!(
            "{kind} id must contain one to {MAX_ID_CHARS} characters"
        )));
    }
    Ok(())
}

fn validate_text(kind: &str, value: &str, max_chars: usize) -> Result<(), RuntimeError> {
    let count = value.chars().count();
    if value.trim().is_empty() || count > max_chars {
        return Err(RuntimeError::config(format!(
            "{kind} must contain one to {max_chars} characters"
        )));
    }
    Ok(())
}

fn bound_string(value: String, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> InteractionRequest {
        InteractionRequest::questionnaire(
            InteractionRequestId::new("interaction-1"),
            InteractionOrigin::new(
                SessionId::new("session-1"),
                TurnId::new("turn-1"),
                ToolCallId::new("call-1"),
            ),
            Questionnaire::new(vec![
                Question::new(
                    QuestionId::new("implementation"),
                    "Implementation",
                    "Which?",
                )
                .with_choices(vec![
                    Choice::new(ChoiceId::new("a"), "A"),
                    Choice::new(ChoiceId::new("b"), "B"),
                ]),
                Question::new(QuestionId::new("detail"), "Detail", "Any detail?")
                    .allow_free_form(true),
            ])
            .unwrap(),
            Deadline::never(),
            InteractionSensitivity::Sensitive,
        )
        .unwrap()
    }

    #[test]
    fn valid_answer_round_trips_and_debug_redacts_free_form() {
        let request = request();
        let response = InteractionResponse::answered(
            request.id().clone(),
            vec![
                QuestionAnswer::choice(QuestionId::new("implementation"), ChoiceId::new("a")),
                QuestionAnswer::free_form(QuestionId::new("detail"), "private value"),
            ],
        );
        response.validate_for(&request).unwrap();
        let debug = format!("{request:?} {response:?}");
        assert!(!debug.contains("Which?"));
        assert!(!debug.contains("private value"));
        assert!(debug.contains("answer_count"));

        let json = serde_json::to_string(&response).unwrap();
        let restored: InteractionResponse = serde_json::from_str(&json).unwrap();
        restored.validate_for(&request).unwrap();
        assert_eq!(restored, response);
    }

    #[test]
    fn response_rejects_identity_duplicates_foreign_and_missing_answers() {
        let request = request();
        assert!(
            InteractionResponse::declined(InteractionRequestId::new("other"))
                .validate_for(&request)
                .is_err()
        );
        assert!(
            InteractionResponse::answered(
                request.id().clone(),
                vec![QuestionAnswer::choice(
                    QuestionId::new("implementation"),
                    ChoiceId::new("a"),
                )],
            )
            .validate_for(&request)
            .is_err()
        );
        assert!(
            InteractionResponse::answered(
                request.id().clone(),
                vec![
                    QuestionAnswer::choice(QuestionId::new("implementation"), ChoiceId::new("a"),),
                    QuestionAnswer::free_form(QuestionId::new("implementation"), "duplicate",),
                ],
            )
            .validate_for(&request)
            .is_err()
        );
        assert!(
            InteractionResponse::answered(
                request.id().clone(),
                vec![
                    QuestionAnswer::choice(
                        QuestionId::new("implementation"),
                        ChoiceId::new("foreign"),
                    ),
                    QuestionAnswer::free_form(QuestionId::new("detail"), "ok",),
                ],
            )
            .validate_for(&request)
            .is_err()
        );
    }

    #[test]
    fn questionnaire_bounds_and_duplicate_ids_fail_closed() {
        assert!(
            Questionnaire::new(Vec::new()).is_err(),
            "an empty questionnaire cannot wait on a host"
        );
        assert!(
            Questionnaire::new(vec![
                Question::new(QuestionId::new("same"), "One", "One?").allow_free_form(true),
                Question::new(QuestionId::new("same"), "Two", "Two?").allow_free_form(true),
            ])
            .is_err()
        );
        assert!(
            Questionnaire::new(vec![
                Question::new(QuestionId::new("q"), "Choice", "Choose").with_choices(vec![
                    Choice::new(ChoiceId::new("same"), "A"),
                    Choice::new(ChoiceId::new("same"), "B"),
                ],),
            ])
            .is_err()
        );
    }

    #[test]
    fn request_and_response_wire_versions_are_required() {
        let request = request();
        let mut request_json = serde_json::to_value(&request).unwrap();
        request_json
            .as_object_mut()
            .unwrap()
            .remove("schema_version");
        assert!(serde_json::from_value::<InteractionRequest>(request_json).is_err());

        let mut request_json = serde_json::to_value(&request).unwrap();
        request_json.as_object_mut().unwrap().remove("sensitivity");
        assert!(serde_json::from_value::<InteractionRequest>(request_json).is_err());

        let response = InteractionResponse::declined(request.id().clone());
        let mut response_json = serde_json::to_value(response).unwrap();
        response_json
            .as_object_mut()
            .unwrap()
            .remove("schema_version");
        assert!(serde_json::from_value::<InteractionResponse>(response_json).is_err());
    }

    #[test]
    fn answered_debug_never_exposes_semantic_choice_ids() {
        let request = request();
        let response = InteractionResponse::answered(
            request.id().clone(),
            vec![
                QuestionAnswer::choice(
                    QuestionId::new("implementation"),
                    ChoiceId::new("secret-selected-option"),
                ),
                QuestionAnswer::free_form(QuestionId::new("detail"), "secret text"),
            ],
        );
        let debug = format!("{response:?}");
        let outcome_debug = format!("{:?}", response.outcome());
        let answer_debug = format!("{:?}", response.answers().unwrap().first().unwrap());
        assert!(!debug.contains("secret-selected-option"));
        assert!(!debug.contains("secret text"));
        assert!(!outcome_debug.contains("secret-selected-option"));
        assert!(!outcome_debug.contains("secret text"));
        assert!(!answer_debug.contains("implementation"));
        assert!(!answer_debug.contains("secret-selected-option"));
        assert!(debug.contains("answer_count"));
        assert!(outcome_debug.contains("answer_count"));
        assert!(answer_debug.contains("answer_kind"));
    }

    #[test]
    fn request_payload_debug_exposes_only_shape_metadata() {
        let questionnaire = Questionnaire::new(vec![
            Question::new(
                QuestionId::new("secret-question-id"),
                "secret header",
                "secret prompt",
            )
            .with_choices(vec![
                Choice::new(ChoiceId::new("secret-choice-id"), "secret label")
                    .with_description("secret description"),
            ]),
        ])
        .unwrap();
        let request = InteractionRequest::questionnaire(
            InteractionRequestId::new("interaction-1"),
            InteractionOrigin::new(
                SessionId::new("session-1"),
                TurnId::new("turn-1"),
                ToolCallId::new("call-1"),
            ),
            questionnaire.clone(),
            Deadline::never(),
            InteractionSensitivity::Sensitive,
        )
        .unwrap();

        let choice_debug = format!("{:?}", questionnaire.questions()[0].choices()[0]);
        let question_debug = format!("{:?}", questionnaire.questions()[0]);
        let questionnaire_debug = format!("{questionnaire:?}");
        let request_debug = format!("{request:?}");
        for debug in [
            &choice_debug,
            &question_debug,
            &questionnaire_debug,
            &request_debug,
        ] {
            assert!(!debug.contains("secret-question-id"));
            assert!(!debug.contains("secret-choice-id"));
            assert!(!debug.contains("secret header"));
            assert!(!debug.contains("secret prompt"));
            assert!(!debug.contains("secret label"));
            assert!(!debug.contains("secret description"));
            assert!(!debug.contains(request.fingerprint().as_str()));
        }
        assert!(choice_debug.contains("label_chars"));
        assert!(question_debug.contains("choice_count"));
        assert!(questionnaire_debug.contains("question_count"));
        assert!(request_debug.contains("question_count"));
    }
}
